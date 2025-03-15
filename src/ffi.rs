use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_longlong};
use std::ptr;
use std::sync::Arc;

use chrono::DateTime;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use serde_json;

use crate::as_of::AsOf::{Current, EventTime};
use crate::lakehouse::Lakehouse;

pub struct LakehouseHandle {
    lakehouse: Lakehouse,
}

pub struct ArrowBatchHandle {
    batch: arrow_array::RecordBatch,
}

#[no_mangle]
pub extern "C" fn create_lakehouse(path: *const c_char) -> *mut LakehouseHandle {
    let c_str = unsafe {
        assert!(!path.is_null());
        CStr::from_ptr(path)
    };
    
    let path_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    
    let curr_dir = Path::from(path_str);
    let local_store = Arc::new(LocalFileSystem::new());
    
    let lakehouse = Lakehouse::new(curr_dir, local_store);
    
    Box::into_raw(Box::new(LakehouseHandle { lakehouse }))
}

#[no_mangle]
pub extern "C" fn scan_table_current(
    handle: *mut LakehouseHandle, 
    table_name: *const c_char
) -> *mut ArrowBatchHandle {
    let handle = unsafe {
        assert!(!handle.is_null());
        &mut *handle
    };
    
    let c_str = unsafe {
        assert!(!table_name.is_null());
        CStr::from_ptr(table_name)
    };
    
    let table_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };
    
    let result = match rt.block_on(handle.lakehouse.scan(table_str, Current)) {
        Ok(batch) => batch,
        Err(_) => return ptr::null_mut(),
    };
    
    Box::into_raw(Box::new(ArrowBatchHandle { batch: result }))
}

#[no_mangle]
pub extern "C" fn scan_table_as_of(
    handle: *mut LakehouseHandle, 
    table_name: *const c_char,
    timestamp_millis: c_longlong
) -> *mut ArrowBatchHandle {
    let handle = unsafe {
        assert!(!handle.is_null());
        &mut *handle
    };
    
    let c_str = unsafe {
        assert!(!table_name.is_null());
        CStr::from_ptr(table_name)
    };
    
    let table_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    
    let dt = match DateTime::from_timestamp_millis(timestamp_millis) {
        Some(dt) => dt,
        None => return ptr::null_mut(),
    };
    
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };
    
    let result = match rt.block_on(handle.lakehouse.scan(table_str, EventTime(dt))) {
        Ok(batch) => batch,
        Err(_) => return ptr::null_mut(),
    };
    
    Box::into_raw(Box::new(ArrowBatchHandle { batch: result }))
}

#[no_mangle]
pub extern "C" fn get_batch_json(
    batch_handle: *mut ArrowBatchHandle
) -> *mut c_char {
    use arrow_array::cast::AsArray;
    use arrow_array::types::TimestampMillisecondType;
    
    let batch_handle = unsafe {
        if batch_handle.is_null() {
            return ptr::null_mut();
        }
        &mut *batch_handle
    };
    
    let batch = &batch_handle.batch;
    let key_arr = batch.column(0).as_string::<i32>();
    let val_arr = batch.column(1).as_string::<i32>();
    let ts_arr = batch.column(2).as_primitive::<TimestampMillisecondType>();
    
    let mut entries = Vec::new();
    
    for row_idx in 0..batch.num_rows() {
        let key_val = key_arr.value(row_idx).to_owned();
        let val_val = val_arr.value(row_idx).to_owned();
        let ts_val = ts_arr.value(row_idx);
        
        let entry = serde_json::json!({
            "key": key_val,
            "value": val_val,
            "timestamp": ts_val
        });
        
        entries.push(entry);
    }
    
    let json_result = match serde_json::to_string(&entries) {
        Ok(json) => json,
        Err(_) => return ptr::null_mut(),
    };
    
    match CString::new(json_result) {
        Ok(c_str) => c_str.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr);
        }
    }
}

#[no_mangle]
pub extern "C" fn destroy_arrow_batch(ptr: *mut ArrowBatchHandle) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(ptr);
        }
    }
}

#[no_mangle]
pub extern "C" fn destroy_lakehouse(ptr: *mut LakehouseHandle) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(ptr);
        }
    }
}