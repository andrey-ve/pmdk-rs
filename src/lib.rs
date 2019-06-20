//
// Copyright (c) 2019 RepliXio Ltd. All rights reserved.
// Use is subject to license terms.
//

#![doc(html_root_url = "https://docs.rs/pmdk/0.0.6")]

use crossbeam_queue::ArrayQueue;
use libc::{c_char, c_int, c_void};
use libc::{mode_t, size_t};
use std::convert::{From, Into, TryInto};
use std::ffi::CString;
use std::mem;
use std::option::Option;
use std::path::Path;
use std::sync::Arc;

use pmdk_sys::obj::{
    pmemobj_alloc, pmemobj_close, pmemobj_create, pmemobj_direct, pmemobj_free,
    pmemobj_memcpy_persist, PMEMobjpool as SysPMEMobjpool,
};
use pmdk_sys::PMEMoid;

use crate::error::WrapErr;

pub use crate::error::{Error, Kind as ErrorKind};

mod error;

// TODO: need this or null func pointer
#[no_mangle]
unsafe extern "C" fn obj_constr_none(
    _pop: *mut SysPMEMobjpool,
    _ptr: *mut c_void,
    _arg: *mut c_void,
) -> c_int {
    0
}

fn alloc(pop: *mut SysPMEMobjpool, size: usize, data_type: u64) -> Result<PMEMoid, Error> {
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
        Err(Error::obj_error())
    }
}

fn alloc_multi(
    pool: Arc<ObjPool>,
    queue: Arc<ArrayQueue<ObjRawKey>>,
    size: usize,
    data_type: u64,
    nobjects: usize,
) -> Result<(), Error> {
    let mut queue = queue;
    (0..nobjects)
        .map(|_| {
            if let Some(_) = Arc::get_mut(&mut queue) {
                Err(ErrorKind::PmdkDropBeforeAllocationError.into())
            } else {
                let oid = alloc(pool.inner, size, data_type)?;
                queue
                    .push(oid.into())
                    .wrap_err(ErrorKind::PmdkNoSpaceInQueueError)
            }
        })
        .collect::<Result<Vec<()>, Error>>()
        .map(|_| ())
}

const OBJ_HEADER_SIZE: usize = 64;
const OBJ_ALLOC_FACTOR: usize = 3;

fn pool_size(capacity: usize, obj_size: usize) -> usize {
    (capacity * (obj_size + OBJ_HEADER_SIZE)) * OBJ_ALLOC_FACTOR
}

const DIRECT_PTR_MASK: u64 = !0u64 >> ((mem::size_of::<u64>() * 8 - 4) as u64);

#[derive(Debug)]
pub struct ObjRawKey(*mut c_void);

impl From<*mut c_void> for ObjRawKey {
    fn from(p: *mut c_void) -> Self {
        ObjRawKey(p)
    }
}

impl From<u64> for ObjRawKey {
    fn from(o: u64) -> Self {
        let p = unsafe { mem::transmute::<_, *mut c_void>(o) };
        p.into()
    }
}

impl From<ObjRawKey> for u64 {
    fn from(key: ObjRawKey) -> Self {
        let p = key.0;
        unsafe { mem::transmute::<_, u64>(p) }
    }
}

impl From<PMEMoid> for ObjRawKey {
    fn from(oid: PMEMoid) -> Self {
        ObjRawKey(unsafe { pmemobj_direct(oid) })
    }
}

impl ObjRawKey {
    const fn as_ptr(&self) -> *const c_void {
        self.0 as *const c_void
    }

    fn as_mut_ptr(&mut self) -> *mut c_void {
        self.0
    }
}

unsafe impl std::marker::Send for ObjRawKey {}
unsafe impl std::marker::Sync for ObjRawKey {}

pub struct ObjPool {
    inner: *mut SysPMEMobjpool,
    uuid_lo: u64,
}

unsafe impl std::marker::Send for ObjPool {}
unsafe impl std::marker::Sync for ObjPool {}

impl ObjPool {
    fn with_layout<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        size: usize,
    ) -> Result<*mut SysPMEMobjpool, Error> {
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
            Err(Error::obj_error())
        } else {
            Ok(inner)
        }
    }

    pub fn new<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        size: usize,
    ) -> Result<Self, Error> {
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
    ) -> Result<(Self, ArrayQueue<ObjRawKey>), Error> {
        let poolsize = pool_size(capacity, obj_size);
        let pool = Self::new(path, layout, poolsize)?;
        let inner = pool.inner;
        let aqueue = ArrayQueue::new(capacity);

        /* pre allocate as much objects as possible up to capacity */
        let _ = (0..capacity)
            .take_while(|_| {
                alloc(inner, obj_size, 1)
                    .map(|oid| aqueue.push(oid.into()))
                    .is_ok()
            })
            .collect::<Vec<_>>();
        Ok((pool, aqueue))
    }

    pub fn with_initial_capacity<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        obj_size: usize,
        capacity: usize,
        initial_capacity: usize,
    ) -> Result<(Arc<Self>, Arc<ArrayQueue<ObjRawKey>>), Error> {
        let initial_capacity = if capacity < initial_capacity {
            // TODO: log it
            capacity
        } else {
            initial_capacity
        };
        let poolsize = pool_size(capacity, obj_size);
        let pool = Arc::new(Self::new(path, layout, poolsize)?);
        let aqueue = Arc::new(ArrayQueue::new(capacity));
        alloc_multi(
            Arc::clone(&pool),
            Arc::clone(&aqueue),
            obj_size,
            1,
            initial_capacity,
        )?;
        if capacity > initial_capacity {
            let alloc_pool = Arc::clone(&pool);
            let alloc_queue = Arc::clone(&aqueue);
            std::thread::spawn(move || {
                alloc_multi(
                    alloc_pool,
                    alloc_queue,
                    obj_size,
                    1,
                    capacity - initial_capacity,
                )
                .expect("PMEM object pool allocation thread");
            });
        }
        Ok((pool, aqueue))
    }

    pub fn update_by_rawkey<O>(
        &self,
        rkey: ObjRawKey,
        data: &[u8],
        offset: O,
    ) -> Result<ObjRawKey, Error>
    where
        O: Into<Option<usize>>,
    {
        let offset = offset
            .into()
            .unwrap_or_default()
            .try_into()
            .wrap_err(ErrorKind::GenericError)?;
        let src = data.as_ptr() as *const c_void;
        let size = size_t::from(data.len());
        let mut rkey = rkey;
        unsafe {
            let dest = rkey.as_mut_ptr().offset(offset);
            pmemobj_memcpy_persist(self.inner, dest, src, size);
        }
        Ok(rkey)
    }

    pub fn put(&self, data: &[u8], data_type: u64) -> Result<ObjRawKey, Error> {
        let oid = alloc(self.inner, data.len(), data_type)?;
        self.update_by_rawkey(oid.into(), data, None)
    }

    pub unsafe fn get_by_rawkey(&self, rkey: ObjRawKey, buf: &mut [u8]) -> ObjRawKey {
        buf.copy_from_slice(std::slice::from_raw_parts(
            rkey.as_ptr() as *const u8,
            buf.len(),
        ));
        rkey
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
    use crossbeam_queue::ArrayQueue;
    use std::mem;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;

    use super::{DIRECT_PTR_MASK, ObjPool};
    use crate::error::{Kind as ErrorKind, WrapErr};
    use crate::{Error, ObjRawKey};

    fn verify_objs(
        obj_pool: &ObjPool,
        keys_vals: Vec<(ObjRawKey, Vec<u8>)>,
    ) -> Result<Vec<(ObjRawKey, Vec<u8>)>, Error> {
        keys_vals
            .into_iter()
            .map(|(rkey, val)| {
                let mut buf = Vec::with_capacity(val.len());
                let key = unsafe {
                    buf.set_len(val.len());
                    obj_pool.get_by_rawkey(rkey, &mut buf)
                };
                if buf == val {
                    Ok((key, val))
                } else {
                    Err(ErrorKind::GenericError.into())
                }
            })
            .collect::<Result<Vec<(ObjRawKey, Vec<u8>)>, Error>>()
    }

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

        let keys_vals = (0..100)
            .map(|i| {
                let buf = vec![0xafu8; 0x10]; // 16 byte
                let key = obj_pool.put(&buf, i as u64);
                assert!(key.is_ok());
                let key = key.unwrap();

                let key = u64::from(key);

                if key & DIRECT_PTR_MASK != 0u64 {
                    println!(
                        "create:: verification error key 0x{:x} bx{:b} mask 0x{:b} result 0x{:b}",
                        key,
                        key,
                        DIRECT_PTR_MASK,
                        key & DIRECT_PTR_MASK
                    );
                }
                (key.into(), buf)
            })
            .collect::<Vec<_>>();
        println!("create:: MEM put: done!");

        verify_objs(&obj_pool, keys_vals);
        println!("create:: MEM get: done!");
    }

    fn pool_create_with_capacity(
        file_name: &str,
        obj_size: usize,
        capacity: usize,
    ) -> Result<(ObjPool, ArrayQueue<ObjRawKey>), Error> {
        let path = PathBuf::from_str(file_name).unwrap();
        ObjPool::with_capacity::<_, String>(&path, None, obj_size, capacity)
    }

    fn pool_create_with_capacity_differed(
        file_name: &str,
        obj_size: usize,
        capacity: usize,
        initial_capacity: usize,
    ) -> Result<(Arc<ObjPool>, Arc<ArrayQueue<ObjRawKey>>), Error> {
        let path = PathBuf::from_str(file_name).unwrap();
        ObjPool::with_initial_capacity::<_, String>(
            &path,
            None,
            obj_size,
            capacity,
            initial_capacity,
        )
    }

    fn update_objs(
        obj_pool: Arc<ObjPool>,
        aqueue: Arc<ArrayQueue<ObjRawKey>>,
        nobj: usize,
        name: &str,
    ) -> Result<(), Error> {
        let keys_vals: Vec<(ObjRawKey, Vec<u8>)> = (0..nobj)
            .map(|_| {
                let buf = vec![0xafu8; 0x1000]; // 4k
                let key = aqueue.pop().wrap_err(ErrorKind::GenericError)?;
                let key = obj_pool.update_by_rawkey(key, &buf, None)?;
                Ok((key, buf))
            })
            .collect::<Result<Vec<(ObjRawKey, Vec<u8>)>, Error>>()?;
        println!("{}:: MEM put: done!", name);

        let keys_vals = verify_objs(&obj_pool, keys_vals)?;
        println!("{}:: MEM get: done!", name);

        let keys_vals: Vec<(ObjRawKey, Vec<u8>)> = keys_vals
            .into_iter()
            .map(|(key, _)| {
                let mut buf1 = vec![0xafu8; 0x800]; // 2k
                let mut buf2 = vec![0xbcu8; 0x800]; // 2k
                let key = obj_pool.update_by_rawkey(key, &buf2, buf1.len())?;
                buf1.append(&mut buf2);
                Ok((key, buf1))
            })
            .collect::<Result<Vec<(ObjRawKey, Vec<u8>)>, Error>>()?;
        println!("{}:: MEM put partial: done!", name);

        verify_objs(&obj_pool, keys_vals)?;
        println!("{}:: MEM get partial: done!", name);
        Ok(())
    }

    #[test]
    fn preallocate() -> Result<(), Error> {
        let (obj_pool, aqueue) =
            pool_create_with_capacity("__pmdk_basic__preallocate_test.obj", 0x1000, 0x800)?;

        println!("preallocate:: allocated {} objects", aqueue.len());

        let obj_pool = Arc::new(obj_pool);
        let aqueue = Arc::new(aqueue);

        update_objs(
            Arc::clone(&obj_pool),
            Arc::clone(&aqueue),
            10,
            "preallocate",
        )
    }

    #[test]
    fn preallocate_differed() -> Result<(), Error> {
        let mut capacity = 0x800;
        let (obj_pool, aqueue) = pool_create_with_capacity_differed(
            "__pmdk_basic__preallocate_differed_test.obj",
            0x1000,
            0x800,
            0x100,
        )?;

        println!("preallocate differed:: allocated {} objects", aqueue.len());

        update_objs(
            Arc::clone(&obj_pool),
            Arc::clone(&aqueue),
            10,
            "preallocate differed",
        )?;
        capacity -= 10;

        update_objs(
            Arc::clone(&obj_pool),
            Arc::clone(&aqueue),
            100,
            "preallocate differed",
        )?;
        capacity -= 100;

        loop {
            let cur_capacity = aqueue.len();
            if cur_capacity < capacity {
                println!(
                    "preallocate differed:: allocated {} objects (sleep 5)",
                    cur_capacity
                );
                std::thread::sleep(std::time::Duration::from_secs(5));
            } else {
                break;
            }
        }

        println!(
            "preallocate differed:: queue fully allocated {} objects",
            aqueue.len()
        );
        Ok(())
    }

    struct AlignedData {
        _f1: u64,
        _f2: u64,
    }

    #[test]
    fn alloc_alignment() -> Result<(), Error> {
        let obj_size = mem::size_of::<AlignedData>();
        let (obj_pool, aqueue) = pool_create_with_capacity(
            "__pmdk_basic__alignment_test.obj",
            obj_size,
            0x90000 / obj_size,
        )?;

        println!(
            "alloc_alignment:: allocated {} objects of {}",
            aqueue.len(),
            obj_size
        );

        while let Ok(rkey) = aqueue.pop() {
            let key: u64 = unsafe { mem::transmute(rkey) };
            assert_eq!(key & DIRECT_PTR_MASK, 0u64);
        }

        println!("alloc_alignment:: check done!");
        Ok(())
    }
}
