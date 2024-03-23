pub struct Json;

// #[async_trait(? Send)]
// impl Filter for Json {
//     async fn process(&self, mut data: Vec<LogMessage>) -> Vec<LogMessage> {
//         for mut x in &mut data {
//             let json_value: Value = serde_json::from_str(&x.body).unwrap();
//             let map = json_value.as_object().unwrap();
//             x.log = Some(map.get("log").unwrap().as_str().unwrap().trim().to_string());
//             x.map = Some(map.to_owned());
//             println!("{}", x.log.to_owned().unwrap().trim())
//         }
//         data
//     }
// }