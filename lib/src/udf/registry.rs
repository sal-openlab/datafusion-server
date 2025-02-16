// udf/registry.rs

use datafusion::execution::context::SessionContext;

use crate::udf::array_object;

pub fn register(ctx: &SessionContext) {
    log::debug!("Register UDF to runtime environment");
    ctx.register_udf(array_object::array_object_udf().clone());
}
