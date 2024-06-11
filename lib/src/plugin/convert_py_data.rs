// convert_py_data.rs - Converts rust from to Python data structure.
// Sasaki, Naoki <nsasaki@sal.co.jp> February 23, 2023
//

#[cfg(feature = "plugin")]
use pyo3::{
    types::{PyAnyMethods, PyDict, PyDictMethods, PyString},
    Bound, IntoPy, Py, PyAny, PyErr, Python, ToPyObject,
};

#[cfg(feature = "plugin")]
pub fn append_to_py_dict(
    py: Python,
    json_any_array: &[&serde_json::Value],
    py_dict: &Bound<'_, PyAny>,
) -> Result<(), PyErr> {
    for json_any in json_any_array {
        match json_any {
            serde_json::Value::Object(json_obj) => {
                for (key, value) in json_obj {
                    py_dict.set_item(key, to_py_any(py, value)?)?;
                }
            }
            _ => py_dict.set_item("value", to_py_any(py, json_any)?)?,
        }
    }

    Ok(())
}

#[cfg(feature = "plugin")]
fn to_py_any(py: Python, value: &serde_json::Value) -> Result<Py<PyAny>, PyErr> {
    Ok(match value {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(v) => v.to_object(py),
        serde_json::Value::String(v) => PyString::new_bound(py, v).to_object(py),
        serde_json::Value::Number(v) => v.as_f64().to_object(py),
        serde_json::Value::Array(values) => {
            let mut py_list = Vec::<Py<PyAny>>::new();
            for value in values {
                py_list.push(to_py_any(py, value)?);
            }
            py_list.to_object(py)
        }
        serde_json::Value::Object(map) => {
            let py_dict = PyDict::new_bound(py);
            for (key, value) in map {
                py_dict.set_item(key, to_py_any(py, value)?)?;
            }
            py_dict.into_py(py)
        }
    })
}
