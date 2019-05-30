use libc::{c_char, c_int, c_void};
use libc::{mode_t, size_t};
use std::ffi::{CStr, CString};

use std::path::Path;

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

pub struct ObjPool {
    inner: *mut SysPMEMobjpool,
    uuid_lo: u64,
}

// TODO: define error

impl ObjPool {
    pub fn new<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        size: usize,
    ) -> Result<Self, ()> {
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
            // Can't reach sys_pool->uuid_lo field => allocating object to get it
            // TODO: use root object for this workaround
            alloc(inner, 1, 10000).map(|mut oid| {
                let uuid_lo = oid.pool_uuid_lo();
                unsafe { pmemobj_free(&mut oid as *mut PMEMoid) };
                Self { inner, uuid_lo }
            })
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
        println!("MEM pool create: done!");

        let mut keys_vals = (0..10)
            .map(|i| {
                let buf = vec![0xafu8; 0x400]; // 4k
                let key = obj_pool.put(&buf, i as u64);
                assert!(key.is_ok());
                (key.unwrap(), buf)
            })
            .collect::<Vec<_>>();
        println!("MEM put: done!");

        keys_vals.into_iter().map(|(key, val)| {
            let mut buf = Vec::with_capacity(val.len());
            unsafe { obj_pool.get(key, &mut buf) };
            assert_eq!(buf, val);
        });
        println!("MEM get: done!");
    }
}
