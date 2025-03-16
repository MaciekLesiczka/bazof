use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bazof::AsOf;
use bazof::Lakehouse;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryStream;
use object_store::path::Path;
use object_store::ObjectStore;

/// A TableProvider implementation for Bazof lakehouse format
pub struct BazofTableProvider {
    lakehouse: Arc<Lakehouse>,
    table_name: String,
    as_of: AsOf,
    schema: SchemaRef,
}

impl Clone for BazofTableProvider {
    fn clone(&self) -> Self {
        Self {
            lakehouse: self.lakehouse.clone(),
            table_name: self.table_name.clone(),
            as_of: self.as_of,
            schema: self.schema.clone(),
        }
    }
}

impl Debug for BazofTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BazofTableProvider(table={}, as_of={:?})", self.table_name, self.as_of)
    }
}

impl BazofTableProvider {
    /// Create a new BazofTableProvider
    pub fn new(
        store_path: Path, 
        store: Arc<dyn ObjectStore>, 
        table_name: String, 
        as_of: AsOf
    ) -> Result<Self> {
        let lakehouse = Arc::new(Lakehouse::new(store_path, store));
        
        // Create a schema for the table - we know bazof tables always have the same schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        ]));
        
        Ok(Self {
            lakehouse,
            table_name,
            as_of,
            schema,
        })
    }
    
    /// Create a new BazofTableProvider with the current version
    pub fn current(
        store_path: Path, 
        store: Arc<dyn ObjectStore>, 
        table_name: String
    ) -> Result<Self> {
        Self::new(store_path, store, table_name, AsOf::Current)
    }
    
    /// Create a new BazofTableProvider with a specific event time
    pub fn as_of(
        store_path: Path, 
        store: Arc<dyn ObjectStore>, 
        table_name: String,
        event_time: chrono::DateTime<chrono::Utc>
    ) -> Result<Self> {
        Self::new(store_path, store, table_name, AsOf::EventTime(event_time))
    }
}

#[async_trait]
impl TableProvider for BazofTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create a physical execution plan that will lazily read the table
        let exec = BazofExec::new(
            self.lakehouse.clone(),
            self.table_name.clone(),
            self.as_of,
            projection.cloned(),
            self.schema.clone(),
        );
        
        Ok(Arc::new(exec))
    }
}

/// An ExecutionPlan implementation for Bazof lakehouse format
pub struct BazofExec {
    lakehouse: Arc<Lakehouse>,
    table_name: String,
    as_of: AsOf,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl Debug for BazofExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BazofExec")
            .field("table_name", &self.table_name)
            .field("as_of", &self.as_of)
            .field("projection", &self.projection)
            .finish()
    }
}

impl Clone for BazofExec {
    fn clone(&self) -> Self {
        Self {
            lakehouse: self.lakehouse.clone(),
            table_name: self.table_name.clone(),
            as_of: self.as_of,
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
        }
    }
}

impl BazofExec {
    /// Create a new BazofExec
    pub fn new(
        lakehouse: Arc<Lakehouse>,
        table_name: String,
        as_of: AsOf,
        projection: Option<Vec<usize>>,
        schema: SchemaRef,
    ) -> Self {
        // Compute the projected schema
        let projected_schema = match &projection {
            Some(indices) => {
                let fields = indices
                    .iter()
                    .map(|i| schema.field(*i).clone())
                    .collect::<Vec<_>>();
                Arc::new(Schema::new(fields))
            }
            None => schema,
        };
        
        Self {
            lakehouse,
            table_name,
            as_of,
            projected_schema,
            projection,
        }
    }
}

impl DisplayAs for BazofExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "BazofExec: table={}, as_of={:?}", self.table_name, self.as_of)
    }
}

impl ExecutionPlan for BazofExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    
    fn name(&self) -> &str {
        "BazofExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }


    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Clone values needed for the execution
        let lakehouse = self.lakehouse.clone();
        let table_name = self.table_name.clone();
        let as_of = self.as_of;
        let projection = self.projection.clone();
        let schema = self.projected_schema.clone();
        
        // Create a one-shot task that fetches data
        let fut = async move {
            // Fetch the data from Bazof
            let batch = lakehouse.scan(&table_name, as_of).await
                .map_err(|e| DataFusionError::Execution(format!("Error scanning Bazof table: {}", e)))?;
            
            // Apply projection if needed
            let projected_batch = match projection {
                Some(indices) => {
                    // Get only the requested columns
                    let projected_columns = indices
                        .iter()
                        .map(|&i| batch.column(i).clone())
                        .collect::<Vec<_>>();
                    
                    // Create a new batch with the projected columns
                    RecordBatch::try_new(
                        schema,
                        projected_columns,
                    )?
                }
                None => {
                    // Use the batch as is, just ensure it uses the right schema
                    RecordBatch::try_new(
                        schema,
                        batch.columns().to_vec(),
                    )?
                }
            };
            
            Ok(vec![projected_batch])
        };
        
        // Execute the future and get the result
        let batches = futures::executor::block_on(fut)?;
        
        // Create a memory stream from the batches
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.projected_schema.clone(),
            None,
        )?))
    }
}