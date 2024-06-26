use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};

use axum::{
    extract::Query,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use geoutils::Location;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{FromRow, Pool, Sqlite};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    if std::env::args().nth(1) == Some("--version".to_string()) {
        println!(
            "{}",
            option_env!("CARGO_PKG_VERSION").unwrap_or_else(|| "unknown")
        );
        return;
    }

    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "debug,gaia=debug,tower_http=debug");
    }

    tracing_subscriber::fmt::init();

    tracing::info!(
        "Starting gaia v{}",
        option_env!("CARGO_PKG_VERSION").unwrap_or_else(|| "unknown")
    );

    let sqlite_pool: Arc<Pool<Sqlite>> = Arc::new(
        Pool::connect(&env::var("DATABASE_URL").expect("Missing DATABASE_URL"))
            .await
            .unwrap(),
    );

    let app = Router::new()
        .nest(
            "/api",
            Router::new().nest(
                "/v0",
                Router::new()
                    .route("/geocode/reverse", get(get_geo_reverse))
                    .route("/geocode/reverse/bulk", post(post_geo_reverse_bulk)),
            ),
        )
        .layer(Extension(sqlite_pool));
    let bind_address: SocketAddr = env::var("BIND_ADDRESS")
        .unwrap_or_else(|_| String::from("0.0.0.0:8081"))
        .parse()
        .unwrap();
    let listener = tokio::net::TcpListener::bind(bind_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Serialize, Deserialize, FromRow, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct Geocode {
    pub lat: String,
    pub lon: String,
    pub address: sqlx::types::Json<RadarAddress>,
}

#[derive(Serialize, Deserialize, FromRow, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct GeocodeResponse {
    pub lat: String,
    pub lon: String,
    pub distance: f64,
    pub address: RadarAddress,
}

#[derive(Serialize, Deserialize, FromRow, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct RadarReverseGeocodeResponse {
    pub meta: Value,
    pub addresses: Vec<RadarAddress>,
}
#[derive(Serialize, Deserialize, FromRow, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RadarAddress {
    address_label: Option<String>,
    city: Option<String>,
    country: Option<String>,
    country_code: Option<String>,
    county: Option<String>,
    formatted_address: Option<String>,
    latitude: Option<f64>,
    layer: Option<String>,
    longitude: Option<f64>,
    number: Option<String>,
    postal_code: Option<String>,
    state: Option<String>,
    state_code: Option<String>,
    street: Option<String>,
}

async fn get_geo_reverse(
    Query(params): Query<HashMap<String, String>>,
    Extension(pool): Extension<Arc<Pool<Sqlite>>>,
) -> impl IntoResponse {
    let lat = match params.get("lat") {
        Some(lat) => format!("{:.5}", lat.parse::<f64>().unwrap()),
        None => return (StatusCode::BAD_REQUEST, Json(json!("missing lat"))).into_response(),
    };
    let lon = match params.get("lon") {
        Some(lon) => format!("{:.5}", lon.parse::<f64>().unwrap()),
        None => return (StatusCode::BAD_REQUEST, Json(json!("missing lon"))).into_response(),
    };

    return (
        StatusCode::OK,
        Json(geo_reverse(lat, lon, pool).await.unwrap()),
    )
        .into_response();
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BulkGeocodeReverseRequest {
    pub lat: String,
    pub lon: String,
}

async fn post_geo_reverse_bulk(
    Extension(pool): Extension<Arc<Pool<Sqlite>>>,
    Json(data): Json<Vec<BulkGeocodeReverseRequest>>,
) -> impl IntoResponse {
    let mut response = vec![];
    for req in data {
        response.push(geo_reverse(req.lat, req.lon, pool.clone()).await.unwrap())
    }

    (StatusCode::OK, Json(response.into_iter().flatten().collect::<Vec<_>>()))
}

async fn geo_reverse(
    lat: String,
    lon: String,
    pool: Arc<Pool<Sqlite>>,
) -> Result<Vec<GeocodeResponse>, String> {
    let geocodes =
        sqlx::query_as::<_, Geocode>("SELECT * FROM geocode WHERE lat LIKE ? AND lon LIKE ?")
            .bind(format!("{:.4}%", lat))
            .bind(format!("{:.4}%", lon))
            .fetch_all(&*pool)
            .await
            .unwrap()
            .into_iter()
            .map(|g| GeocodeResponse {
                lat: lat.clone(),
                lon: lon.clone(),
                address: g.address.0.clone(),
                distance: Location::new(g.address.latitude.unwrap(), g.address.longitude.unwrap())
                    .distance_to(&Location::new(
                        lat.parse::<f64>().unwrap(),
                        lon.parse::<f64>().unwrap(),
                    ))
                    .unwrap()
                    .meters(),
            })
            .filter(|g| g.distance < 40.0)
            .collect::<Vec<_>>();

    if geocodes.len() > 0 {
        tracing::info!("got from cache");
        return Ok(geocodes);
    }

    let response: RadarReverseGeocodeResponse = ureq::get(&format!(
        "https://api.radar.io/v1/geocode/reverse?coordinates={},{}",
        lat, lon
    ))
    .set(
        "Authorization",
        &env::var("RADAR_API_KEY").expect("Missing RADAR_API_KEY"),
    )
    .call()
    .unwrap()
    .into_json()
    .unwrap();

    for address in response.addresses.iter() {
        sqlx::query("INSERT INTO geocode(lat,lon,address) VALUES (?, ?, ?)")
            .bind(&lat)
            .bind(&lon)
            .bind(json!(address))
            .execute(&*pool)
            .await
            .unwrap();
    }

    return Ok(response
        .addresses
        .iter()
        .map(|a| GeocodeResponse {
            lat: lat.clone(),
            lon: lon.clone(),
            address: a.clone(),
            distance: Location::new(a.latitude.unwrap(), a.longitude.unwrap())
                .distance_to(&Location::new(
                    lat.parse::<f64>().unwrap(),
                    lon.parse::<f64>().unwrap(),
                ))
                .unwrap()
                .meters(),
        })
        .collect::<Vec<_>>());
}
