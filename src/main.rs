use anyhow::Result;
use askama::Template;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use btleplug::{
    api::{Central, CentralEvent, Manager as _, Peripheral, ScanFilter},
    platform::Manager,
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::iter::Iterator;
use std::sync::Arc;
use tokio::sync::RwLock;

// Shared state
#[derive(Clone)]
struct AppState {
    devices: Arc<RwLock<Vec<ScannedDevice>>>,
}

#[derive(Clone, serde::Serialize)]
struct ScannedDevice {
    name: String,
    address: String,
    connected: bool,
    address_type: String,
    tx_power_level: i16,
    rssi: i16,
    manufacturer_data: Vec<(u16, String)>,
    service_data: Vec<(String, String)>,
    services: Vec<String>,
}

impl ScannedDevice {
    fn from_properties(
        id: &btleplug::platform::PeripheralId,
        properties: btleplug::api::PeripheralProperties,
    ) -> Self {
        ScannedDevice {
            name: properties
                .local_name
                .unwrap_or_else(|| "Unknown".to_string()),
            address: id.to_string(),
            connected: false,
            address_type: properties
                .address_type
                .map(|a| format!("{:?}", a))
                .unwrap_or_else(|| "Unknown".to_string()),
            tx_power_level: properties.tx_power_level.unwrap_or(0),
            rssi: properties.rssi.unwrap_or(0),
            manufacturer_data: properties
                .manufacturer_data
                .iter()
                .map(|(k, v)| (k.clone(), format!("{:?}", v)))
                .collect(),
            service_data: properties
                .service_data
                .iter()
                .map(|(k, v)| (k.to_string(), format!("{:?}", v)))
                .collect(),
            services: properties
                .services
                .iter()
                .map(|s| format!("{:?}", s))
                .collect(),
        }
    }
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate;

#[derive(Template)]
#[template(path = "devices.html")]
struct DevicesTemplate {
    devices: Vec<ScannedDevice>,
}

#[derive(Template)]
#[template(path = "characteristics.html")]
struct CharacteristicsTemplate {
    characteristics: Vec<String>,
}

#[derive(Deserialize)]
struct ConnectRequest {
    address: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let manager = Manager::new().await?;
    let adapter = manager.adapters().await?.into_iter().next().unwrap();
    let filter = ScanFilter::default();
    adapter.start_scan(filter).await?;

    let devices = Arc::new(RwLock::new(Vec::new()));
    let state = AppState {
        devices: devices.clone(),
    };

    tokio::spawn(ble_scanner(adapter, devices));

    let app = Router::new()
        .route("/", get(root))
        .route("/devices", get(get_devices))
        .route("/ws/devices", get(websocket_handler))
        .route("/connect", post(connect))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn root() -> impl IntoResponse {
    Html(IndexTemplate.render().unwrap())
}
async fn get_devices(state: axum::extract::State<AppState>) -> impl IntoResponse {
    let devices = state.devices.read().await.clone();
    Html(DevicesTemplate { devices }.render().unwrap())
}

async fn websocket_handler(ws: WebSocketUpgrade, state: State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

async fn handle_websocket(mut socket: WebSocket, state: State<AppState>) {
    loop {
        let devices = state.devices.read().await.clone();
        let rendered_html = DevicesTemplate { devices }.render().unwrap();

        if socket.send(Message::Text(rendered_html)).await.is_err() {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn connect(
    state: axum::extract::State<AppState>,
    axum::extract::Form(form): axum::extract::Form<ConnectRequest>,
) -> impl IntoResponse {
    let address = form.address.clone();

    let devices = state.devices.read().await;

    if let Some(device) = devices.iter().find(|d| d.address == address) {
        let manager = Manager::new().await.unwrap();
        let adapter = manager
            .adapters()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        for p in adapter.peripherals().await.unwrap() {
            if p.properties().await.unwrap().unwrap().address.to_string() == address {
                p.connect().await.unwrap();
                p.discover_services().await.unwrap();

                let characteristics = p
                    .characteristics()
                    .iter()
                    .map(|c| format!("{:?}", c))
                    .collect();
                return Html(
                    CharacteristicsTemplate { characteristics }
                        .render()
                        .unwrap(),
                );
            }
        }
    }
    Html("<p>Device not found.</p>".to_string())
}

async fn ble_scanner(
    adapter: btleplug::platform::Adapter,
    devices: Arc<RwLock<Vec<ScannedDevice>>>,
) {
    let mut events = adapter.events().await.unwrap();

    loop {
        if let Some(event) = events.next().await {
            match event {
                CentralEvent::DeviceDiscovered(id) | CentralEvent::DeviceUpdated(id) => {
                    let mut list = devices.write().await;
                    if let Some(existing) = list.iter_mut().find(|d| d.address == id.to_string()) {
                        // Update existing device properties.
                        if let Ok(peripheral) = adapter.peripheral(&id).await {
                            if let Ok(Some(properties)) = peripheral.properties().await {
                                *existing = ScannedDevice::from_properties(&id, properties);
                            }
                        }
                    } else {
                        // New device discovered.
                        handle_device(&adapter, &id, &mut list).await;
                    }
                }
                _ => {
                    println!("Other event: {:?}", event);
                }
            }
        }
        /*
            if let CentralEvent::DeviceDiscovered(id) = event {
                let mut list = devices.write().await;
                if !list.iter().any(|d| d.address == id.to_string()) {
                    handle_device(&adapter, &id, &mut list).await;
                }
            }
        */

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn handle_device(
    adapter: &btleplug::platform::Adapter,
    id: &btleplug::platform::PeripheralId,
    list: &mut Vec<ScannedDevice>,
) {
    if let Ok(peripheral) = adapter.peripheral(id).await {
        if let Ok(Some(properties)) = peripheral.properties().await {
            let device = ScannedDevice::from_properties(id, properties);
            list.push(device);
        } else {
            println!("Error: Could not retrieve properties for device {id}");
        }
    } else {
        println!("Error: Could not get peripheral for device {id}");
    }
}
