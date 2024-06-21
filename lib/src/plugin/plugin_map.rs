// plugin_map - Creates python plugin code map.
// Sasaki, Naoki <nsasaki@sal.co.jp> February 18, 2023
//

#[cfg(feature = "plugin")]
use crate::plugin::definition::{Definition, PluginType};
#[cfg(feature = "plugin")]
use crate::Settings;
use std::collections::HashMap;
use std::path::PathBuf;

#[allow(dead_code)]
#[derive(Debug)]
pub struct PluginMap {
    // module, (py_file, entry, version)
    pub scheme_py_map: HashMap<String, (PathBuf, String, String)>,
    pub processor_py_map: HashMap<String, (PathBuf, String, String)>,
}

impl PluginMap {
    #[cfg(feature = "plugin")]
    pub fn new() -> anyhow::Result<Self> {
        let mut scheme_py_map = HashMap::<String, (PathBuf, String, String)>::new();
        let mut processor_py_map = HashMap::<String, (PathBuf, String, String)>::new();

        let start_dir = PathBuf::from(&Settings::global().server.plugin_dir);
        log::debug!("Scanning plugin in {:?}", start_dir);
        PluginMap::scan_plugin(&start_dir, &mut scheme_py_map, &mut processor_py_map)?;
        log::debug!("Detected data source plugins: {:?}", scheme_py_map);
        log::debug!("Detected processor plugins: {:?}", processor_py_map);

        Ok(Self {
            scheme_py_map,
            processor_py_map,
        })
    }

    #[cfg(not(feature = "plugin"))]
    #[allow(clippy::unnecessary_wraps)]
    pub fn new() -> anyhow::Result<Self> {
        let scheme_py_map = HashMap::<String, (PathBuf, String, String)>::new();
        let processor_py_map = HashMap::<String, (PathBuf, String, String)>::new();
        Ok(Self {
            scheme_py_map,
            processor_py_map,
        })
    }

    #[cfg(feature = "plugin")]
    fn scan_plugin(
        dir: &PathBuf,
        scheme_map: &mut HashMap<String, (PathBuf, String, String)>,
        processor_map: &mut HashMap<String, (PathBuf, String, String)>,
    ) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();

            if path.is_dir() {
                let def_file = path.join("plugin_def.toml");

                if def_file.exists() {
                    log::debug!("Detect plugin definition {:?}", def_file);

                    let definition = Definition::new(def_file.as_path())?;

                    match definition.general.plugin_type {
                        PluginType::DataSource => {
                            if let Some(scheme) = &definition.general.scheme {
                                if scheme_map.contains_key(scheme.as_str()) {
                                    return Err(anyhow::Error::msg(format!(
                                        "Detect duplicated scheme name '{scheme}' in plugins directory"
                                    )));
                                }
                            } else {
                                return Err(anyhow::Error::msg(format!(
                                    "Must be defined scheme in '{}'",
                                    def_file.to_str().unwrap_or("Unknown")
                                )));
                            }
                        }
                        PluginType::Processor => {
                            if let Some(module) = &definition.general.module {
                                if scheme_map.contains_key(module.as_str()) {
                                    return Err(anyhow::Error::msg(format!(
                                        "Detect duplicated module name '{module}' in plugins directory"
                                    )));
                                }
                            } else {
                                return Err(anyhow::Error::msg(format!(
                                    "Must be defined module in '{}'",
                                    def_file.to_str().unwrap_or("Unknown")
                                )));
                            }
                        }
                    }

                    let py_file = path.join(definition.plugin.file);

                    if !py_file.exists() {
                        return Err(anyhow::Error::msg(format!(
                            "Does not exists plugin file {}",
                            py_file.to_str().unwrap_or("unknown")
                        )));
                    }

                    match definition.general.plugin_type {
                        PluginType::DataSource => {
                            scheme_map.insert(
                                definition.general.scheme.clone().unwrap(),
                                (
                                    py_file,
                                    definition.plugin.entry.clone(),
                                    definition.general.version.clone(),
                                ),
                            );
                        }
                        PluginType::Processor => {
                            processor_map.insert(
                                definition.general.module.clone().unwrap(),
                                (
                                    py_file,
                                    definition.plugin.entry.clone(),
                                    definition.general.version.clone(),
                                ),
                            );
                        }
                    }
                } else {
                    PluginMap::scan_plugin(&path, scheme_map, processor_map)?;
                }
            }
        }

        Ok(())
    }
}
