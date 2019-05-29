use libc::c_char;
use libc::{mode_t, size_t};
use std::ffi::{CStr, CString};

use std::path::Path;

use pmdk_sys::obj::{
    pmemobj_alloc, pmemobj_close, pmemobj_constr, pmemobj_create, pmemobj_errormsg,
    PMEMobjpool as SysPMEMobjpool
};
use pmdk_sys::{pmem_persist, PMEMoid};

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

pub struct ObjPool {
    inner: *mut SysPMEMobjpool,
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
        // TODO: what if NULL returned
        let sys_pool = unsafe {
            pmemobj_create(
                path.as_ptr() as *const c_char,
                layout,
                size_t::from(size),
                mode as mode_t,
            )
        };

        if sys_pool.is_null() {
            let _ = errormsg().map(|msg| println!("pmemobj_create failed: {}", msg));
            Err(())
        } else {
            Ok(Self { inner: sys_pool })
        }
    }

    pub fn put(&self, data: &[u8]) -> Result<u64, ()> {
        unsafe {}

        Err(())
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
        let path = PathBuf::from_str("__pmdk_basic__create_test.obj").unwrap();
        let size = 0x1000000; // 16 Mb

        let obj_pool = ObjPool::new::<_, String>(&path, None, size);
        assert!(obj_pool.is_ok());
    }
}
