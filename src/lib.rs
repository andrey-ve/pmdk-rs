//
// Copyright (c) 2019 RepliXio Ltd. All rights reserved.
// Use is subject to license terms.
//

#![doc(html_root_url = "https://docs.rs/pmdk/0.0.3")]

use libc::{c_char, c_int, c_void};
use libc::{mode_t, size_t};
use std::ffi::{CStr, CString};

use std::path::Path;

use crossbeam_queue::ArrayQueue;

use pmdk_sys::obj::{
    pmemobj_alloc, pmemobj_close, pmemobj_create, pmemobj_direct, pmemobj_errormsg, pmemobj_free,
    pmemobj_memcpy_persist, PMEMobjpool as SysPMEMobjpool,
};
use pmdk_sys::PMEMoid;

fn errormsg() -> Result<String, ()> {
    unsafe {
        let msg = pmemobj_errormsg();
        if msg.is_null() {
            Err(())
        } else {
            CStr::from_ptr(msg).to_owned().into_string().map_err(|_| ())
        }
    }
}

// TODO: need this or null func pointer
#[no_mangle]
unsafe extern "C" fn obj_constr_none(
    _pop: *mut SysPMEMobjpool,
    _ptr: *mut c_void,
    _arg: *mut c_void,
) -> c_int {
    0
}

fn alloc(pop: *mut SysPMEMobjpool, size: usize, data_type: u64) -> Result<PMEMoid, ()> {
    let mut oid = PMEMoid::default();
    let oidp = &mut oid;

    let status = unsafe {
        pmemobj_alloc(
            pop,
            oidp as *mut PMEMoid,
            size_t::from(size),
            data_type,
            obj_constr_none,
            std::ptr::null_mut::<c_void>(),
        )
    };

    if status == 0 {
        Ok(oid)
    } else {
        let _ = errormsg().map(|msg| println!("pmemobj_alloc failed: {}", msg));
        Err(())
    }
}

const OBJ_HEADER_SIZE: usize = 64;
const OBJ_ALLOC_FACTOR: usize = 3;

fn pool_size(capacity: usize, obj_size: usize) -> usize {
    (capacity * (obj_size + OBJ_HEADER_SIZE)) * OBJ_ALLOC_FACTOR
}

pub type ObjRawKey = *mut c_void;

pub struct ObjPool {
    inner: *mut SysPMEMobjpool,
    uuid_lo: u64,
}

// TODO: define error

impl ObjPool {
    fn with_layout<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        size: usize,
    ) -> Result<*mut SysPMEMobjpool, ()> {
        let path = path.as_ref().to_string_lossy();
        // TODO: remove unwrap
        let layout = layout.map_or_else(
            || std::ptr::null::<c_char>(),
            |layout| CString::new(layout.into()).unwrap().as_ptr() as *const c_char,
        );

        let mode = 0o666;
        let inner = unsafe {
            pmemobj_create(
                path.as_ptr() as *const c_char,
                layout,
                size_t::from(size),
                mode as mode_t,
            )
        };

        if inner.is_null() {
            let _ = errormsg().map(|msg| println!("pmemobj_create failed: {}", msg));
            Err(())
        } else {
            Ok(inner)
        }
    }

    pub fn new<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        size: usize,
    ) -> Result<Self, ()> {
        Self::with_layout(path, layout, size).and_then(|inner| {
            // Can't reach sys_pool->uuid_lo field => allocating object to get it
            // TODO: use root object for this workaround
            alloc(inner, 1, 10000).map(|mut oid| {
                let uuid_lo = oid.pool_uuid_lo();
                unsafe { pmemobj_free(&mut oid as *mut PMEMoid) };
                Self { inner, uuid_lo }
            })
        })
    }

    pub fn with_capacity<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        obj_size: usize,
        capacity: usize,
    ) -> Result<(Self, ArrayQueue<ObjRawKey>), ()> {
        let poolsize = pool_size(capacity, obj_size);
        let pool = Self::new(path, layout, poolsize)?;
        let inner = pool.inner;
        let mut aqueue = ArrayQueue::new(capacity);

        /* pre allocate as much objects as possible up to capacity */
        let _ = (0..capacity)
            .take_while(|_| {
                alloc(inner, obj_size, 1)
                    .map(|mut oid| aqueue.push(unsafe { pmemobj_direct(oid) }))
                    .is_ok()
            })
            .collect::<Vec<_>>();
        Ok((pool, aqueue))
    }

    pub fn update_by_rawkey(&self, rkey: ObjRawKey, data: &[u8]) {
        unsafe {
            let src = data.as_ptr() as *const c_void;
            let size = size_t::from(data.len());
            pmemobj_memcpy_persist(self.inner, rkey, src, size);
        }
    }

    fn put_data(&self, oid: PMEMoid, data: &[u8]) {
        unsafe {
            let dest = pmemobj_direct(oid);
            let src = data.as_ptr() as *const c_void;
            let size = size_t::from(data.len());
            pmemobj_memcpy_persist(self.inner, dest, src, size);
        }
    }

    pub fn put(&self, data: &[u8], data_type: u64) -> Result<u64, ()> {
        alloc(self.inner, data.len(), data_type).map(|oid| {
            self.put_data(oid, data);
            oid.off()
        })
    }

    // TODO: this is one of many interfaces for known size data retrieval
    pub unsafe fn get(&self, key: u64, buf: &mut [u8]) {
        let oid = PMEMoid::new(self.uuid_lo, key);
        let raw = pmemobj_direct(oid);
        buf.copy_from_slice(std::slice::from_raw_parts(raw as *const u8, buf.len()));
    }

    pub unsafe fn get_by_rawkey(&self, rkey: ObjRawKey, buf: &mut [u8]) {
        buf.copy_from_slice(std::slice::from_raw_parts(rkey as *const u8, buf.len()));
    }
}

impl Drop for ObjPool {
    fn drop(&mut self) {
        // TODO: remove for debug only
        println!("Dropping obj pool {:?}", self.inner);
        unsafe {
            pmemobj_close(self.inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::str::FromStr;

    use super::ObjPool;

    #[test]
    fn create() {
        let obj_pool = {
            let path = PathBuf::from_str("__pmdk_basic__create_test.obj").unwrap();
            let size = 0x1000000; // 16 Mb

            let obj_pool = ObjPool::new::<_, String>(&path, None, size);
            assert!(obj_pool.is_ok());

            obj_pool.unwrap()
        };
        println!("create:: MEM pool create: done!");

        let mut keys_vals = (0..10)
            .map(|i| {
                let buf = vec![0xafu8; 0x1000]; // 4k
                let key = obj_pool.put(&buf, i as u64);
                assert!(key.is_ok());
                (key.unwrap(), buf)
            })
            .collect::<Vec<_>>();
        println!("create:: MEM put: done!");

        keys_vals.into_iter().for_each(|(key, val)| {
            let mut buf = Vec::with_capacity(val.len());
            unsafe {
                buf.set_len(val.len());
                obj_pool.get(key, &mut buf);
            }
            assert_eq!(buf, val);
        });
        println!("create:: MEM get: done!");
    }

    #[test]
    fn preallocate() {
        let (obj_pool, aqueue) = {
            let path = PathBuf::from_str("__pmdk_basic__preallocate_test.obj").unwrap();
            let obj_size = 0x1000; // 4k
            let capacity = 0x800; // 2k

            let res = ObjPool::with_capacity::<_, String>(&path, None, obj_size, capacity);
            assert!(res.is_ok());

            res.unwrap()
        };

        println!("preallocate:: allocated {} objects", aqueue.len());

        let mut keys_vals = (0..10)
            .map(|_| {
                let buf = vec![0xafu8; 0x1000]; // 4k
                let rkey = aqueue.pop();
                assert!(rkey.is_ok());
                let rkey = rkey.unwrap();
                obj_pool.update_by_rawkey(rkey, &buf);
                (rkey, buf)
            })
            .collect::<Vec<_>>();
        println!("preallocate:: MEM put: done!");

        keys_vals.into_iter().for_each(|(rkey, val)| {
            let mut buf = Vec::with_capacity(val.len());
            unsafe {
                buf.set_len(val.len());
                obj_pool.get_by_rawkey(rkey, &mut buf);
            }
            assert_eq!(buf, val);
        });
        println!("preallocate:: MEM get: done!");
    }
}
