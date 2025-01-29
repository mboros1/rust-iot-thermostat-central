use anyhow::Result;
use askama::Template;
use axum::{
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use btleplug::{
    api::{Central, CentralEvent, Manager as _, Peripheral},
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

#[derive(Template)]
#[template(path = "base.html")]
struct BaseTemplate {
    content: String,
}

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
    // Set up BLE
    let manager = Manager::new().await?;
    let adapter = manager.adapters().await?.into_iter().next().unwrap();
    adapter.start_scan(Default::default()).await?;

    // Shared device list
    let devices = Arc::new(RwLock::new(Vec::new()));
    let state = AppState {
        devices: devices.clone(),
    };

    // Spawn BLE scanner task
    tokio::spawn(ble_scanner(adapter, devices));

    // Set up web server
    let app = Router::new()
        .route("/", get(root))
        .route("/devices", get(get_devices))
        .route("/connect", post(connect))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn root() -> impl IntoResponse {
    Html(
        BaseTemplate {
            content: r#"
                <h1>BLE Device Scanner</h1>
                <div id="devices" hx-get="/devices" hx-trigger="load, every 1s"></div>
            "#
            .to_string(),
        }
        .render()
        .unwrap(),
    )
}

async fn get_devices(state: axum::extract::State<AppState>) -> impl IntoResponse {
    let devices = state.devices.read().await.clone();
    Html(DevicesTemplate { devices }.render().unwrap())
}

async fn connect(
    state: axum::extract::State<AppState>,
    axum::extract::Form(form): axum::extract::Form<ConnectRequest>,
) -> impl IntoResponse {
    // Here you would implement actual BLE connection logic
    let characteristics = vec![
        "Battery Level".to_string(),
        "Device Name".to_string(),
        "Manufacturer".to_string(),
    ];

    Html(
        CharacteristicsTemplate { characteristics }
            .render()
            .unwrap(),
    )
}

async fn ble_scanner(
    adapter: btleplug::platform::Adapter,
    devices: Arc<RwLock<Vec<ScannedDevice>>>,
) {
    let mut events = adapter.events().await.unwrap();

    loop {
        if let Some(event) = events.next().await {
            match event {
                CentralEvent::DeviceDiscovered(id) => {
                    let mut list = devices.write().await;

                    // Check if the device is already in the list
                    if !list.iter().any(|d| d.address == id.to_string()) {
                        // Get the Peripheral for the discovered device
                        let peripheral = adapter.peripheral(&id).await;
                        match peripheral {
                            Err(e) => println!("Error with peripheral for some reason: {e:}"),
                            Ok(peripheral) => {
                                let properties = peripheral.properties().await;
                                match properties {
                                    Ok(properties) => {
                                        let properties = properties.unwrap();
                                        let name = properties
                                            .local_name
                                            .unwrap_or_else(|| "Unknown".to_string());
                                        list.push(ScannedDevice {
                                            name,
                                            address: id.to_string(),
                                            connected: false,
                                            address_type: properties
                                                .address_type
                                                .map(|a| format!("{:?}", a))
                                                .unwrap_or_else(|| "Unknown".to_string()),
                                            tx_power_level: properties
                                                .tx_power_level
                                                .unwrap_or_else(|| 0),
                                            rssi: properties.rssi.unwrap_or_else(|| 0),
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
                                        });
                                    }
                                    Err(e) => println!(
                                        "Error with peripheral properties for some reason: {e:}"
                                    ),
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
