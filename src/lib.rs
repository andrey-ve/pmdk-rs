//
// Copyright (c) 2019 RepliXio Ltd. All rights reserved.
// Use is subject to license terms.
//

#![doc(html_root_url = "https://docs.rs/pmdk/0.6.0")]
#![warn(clippy::use_self)]
#![warn(deprecated_in_future)]
#![warn(future_incompatible)]
#![warn(unreachable_pub)]
#![warn(missing_debug_implementations)]
#![warn(rust_2018_compatibility)]
#![warn(rust_2018_idioms)]
#![warn(unused)]
#![deny(warnings)]
#![feature(test)]

#[allow(unused_extern_crates)]
extern crate test;

use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::path::Path;
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use libc::{c_char, c_void};
use libc::{mode_t, size_t};

use pmdk_sys::obj::{
    pmemobj_alloc, pmemobj_close, pmemobj_create, pmemobj_ctl_exec, pmemobj_ctl_get,
    pmemobj_ctl_set, pmemobj_direct, pmemobj_first, pmemobj_free, pmemobj_memcpy_persist,
    pmemobj_next, pmemobj_oid, pmemobj_type_num, PMEMobjpool as SysPMEMobjpool,
};
pub use pmdk_sys::PMEMoid;

use crate::error::WrapErr;

pub use crate::error::{Error, Kind as ErrorKind};
use pmdk_sys::pmempool_rm;

mod error;

fn str_as_c_char(s: &str) -> Result<*const c_char, Error> {
    CString::new(s)
        .map(|cs| cs.as_ptr() as *const c_char)
        .wrap_err(ErrorKind::GenericError)
}

fn alloc(pop: *mut SysPMEMobjpool, size: usize, data_type: u64) -> Result<PMEMoid, Error> {
    let mut oid = PMEMoid::default();
    let oidp = &mut oid;

    #[allow(clippy::identity_conversion)]
    let size = size_t::from(size);
    let status = unsafe {
        pmemobj_alloc(
            pop,
            oidp as *mut PMEMoid,
            size,
            data_type,
            None,
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
            if Arc::get_mut(&mut queue).is_some() {
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
        .or_else(|e| {
            match e.kind() {
                ErrorKind::PmdkDropBeforeAllocationError => {
                    // TODO: trace it
                    println!("{}", e);
                    Ok(())
                }
                _ => Err(e),
            }
        })
}

const OBJ_HEADER_SIZE: usize = 64;
const OBJ_ALLOC_FACTOR: usize = 3;

fn pool_size(capacity: usize, obj_size: usize) -> usize {
    (capacity * (obj_size + OBJ_HEADER_SIZE)) * OBJ_ALLOC_FACTOR
}

#[derive(Debug)]
pub struct ObjAllocator {
    pool: Arc<ObjPool>,
    obj_size: usize,
    data_type: u64,
    capacity: usize,
    allocated: usize,
}

impl ObjAllocator {
    pub fn allocate(&mut self) -> Result<Option<ObjRawKey>, Error> {
        if self.allocated >= self.capacity {
            Ok(None)
        } else {
            alloc(self.pool.inner, self.obj_size, self.data_type).map(|oid| {
                self.allocated += 1;
                Some(oid.into())
            })
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct ObjRawKey(*mut c_void);

impl From<*mut c_void> for ObjRawKey {
    fn from(p: *mut c_void) -> Self {
        Self(p)
    }
}

impl From<u64> for ObjRawKey {
    fn from(o: u64) -> Self {
        Self(o as *mut c_void)
    }
}

impl From<ObjRawKey> for u64 {
    fn from(key: ObjRawKey) -> Self {
        key.0 as Self
    }
}

impl From<PMEMoid> for ObjRawKey {
    fn from(oid: PMEMoid) -> Self {
        Self(unsafe { pmemobj_direct(oid) })
    }
}

impl From<ObjRawKey> for PMEMoid {
    fn from(key: ObjRawKey) -> Self {
        key.as_persistent()
    }
}

impl ObjRawKey {
    const fn as_ptr(&self) -> *const c_void {
        self.0 as *const c_void
    }

    fn as_mut_ptr(&mut self) -> *mut c_void {
        self.0
    }

    /// # Safety
    /// The ruturned slice should not outlive context of self
    pub unsafe fn as_slice<'a>(&self, len: usize) -> &'a [u8] {
        std::slice::from_raw_parts(self.0 as *const u8, len)
    }

    pub fn as_persistent(&self) -> PMEMoid {
        unsafe { pmemobj_oid(self.as_ptr()) }
    }

    pub fn get_type(&self) -> u64 {
        unsafe { pmemobj_type_num(self.as_persistent()) }
    }

    #[cfg(test)]
    pub fn as_byte_vec(&self) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(
                &(self.0 as u64) as *const u64 as *const u8,
                std::mem::size_of::<u64>(),
            )
        }
        .to_owned()
    }
}

unsafe impl Send for ObjRawKey {}
unsafe impl Sync for ObjRawKey {}

#[derive(Debug)]
pub struct ObjPool {
    inner: *mut SysPMEMobjpool,
    uuid_lo: u64,
    inner_path: CString,
    rm_on_drop: bool,
}

unsafe impl Send for ObjPool {}
unsafe impl Sync for ObjPool {}

impl ObjPool {
    fn with_layout<S: Into<String>>(
        path: &CStr,
        layout: Option<S>,
        size: usize,
    ) -> Result<*mut SysPMEMobjpool, Error> {
        let layout = layout.map_or_else(
            || Ok(std::ptr::null::<c_char>()),
            |layout| {
                CString::new(layout.into())
                    .map(|layout| layout.as_ptr() as *const c_char)
                    .wrap_err(ErrorKind::LayoutError)
            },
        )?;
        #[allow(clippy::identity_conversion)]
        let size = size_t::from(size);
        let mode = 0o666 as mode_t;
        let inner = unsafe { pmemobj_create(path.as_ptr() as *const c_char, layout, size, mode) };

        if inner.is_null() {
            Err(Error::obj_error())
        } else {
            Ok(inner)
        }
    }

    pub fn with_size<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        size: usize,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_str().map_or_else(
            || Err(ErrorKind::PathError.into()),
            |path| CString::new(path).wrap_err(ErrorKind::PathError),
        )?;
        let inner_path = path.clone();
        Self::with_layout(&path, layout, size).and_then(|inner| {
            // Can't reach sys_pool->uuid_lo field => allocating object to get it
            // TODO: use root object for this workaround
            alloc(inner, 1, 10000).map(|mut oid| {
                let uuid_lo = oid.pool_uuid_lo();
                unsafe { pmemobj_free(&mut oid as *mut PMEMoid) };
                Self {
                    inner,
                    uuid_lo,
                    inner_path,
                    rm_on_drop: false,
                }
            })
        })
    }

    pub fn new<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        obj_size: usize,
        capacity: usize,
    ) -> Result<Self, Error> {
        let size = pool_size(capacity, obj_size);
        Self::with_size(path, layout, size)
    }

    pub fn set_capacity(
        self,
        obj_size: usize,
        data_type: u64,
        capacity: usize,
    ) -> Result<(Self, ArrayQueue<ObjRawKey>), Error> {
        let pool = Arc::new(self);
        let aqueue = Arc::new(ArrayQueue::new(capacity));

        alloc_multi(
            Arc::clone(&pool),
            Arc::clone(&aqueue),
            obj_size,
            data_type,
            capacity,
        )
        .and_then(move |_| {
            Arc::try_unwrap(pool)
                .map_err(|_| ErrorKind::GenericError.into())
                .and_then(|pool| {
                    Arc::try_unwrap(aqueue)
                        .map(|aqueue| (pool, aqueue))
                        .map_err(|_| ErrorKind::GenericError.into())
                })
        })
    }

    pub fn with_capacity<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        obj_size: usize,
        data_type: u64,
        capacity: usize,
    ) -> Result<(Self, ArrayQueue<ObjRawKey>), Error> {
        let pool = Self::new(path, layout, obj_size, capacity)?;
        pool.set_capacity(obj_size, data_type, capacity)
    }

    pub fn set_initial_capacity(
        self,
        obj_size: usize,
        data_type: u64,
        capacity: usize,
        initial_capacity: usize,
    ) -> Result<(Arc<Self>, Arc<ArrayQueue<ObjRawKey>>), Error> {
        let initial_capacity = if capacity < initial_capacity {
            // TODO: log it
            capacity
        } else {
            initial_capacity
        };
        let pool = Arc::new(self);
        let aqueue = Arc::new(ArrayQueue::new(capacity));
        alloc_multi(
            Arc::clone(&pool),
            Arc::clone(&aqueue),
            obj_size,
            data_type,
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
                    data_type,
                    capacity - initial_capacity,
                )
                .expect("PMEM object pool allocation thread");
            });
        }
        Ok((pool, aqueue))
    }

    pub fn with_initial_capacity<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        obj_size: usize,
        data_type: u64,
        capacity: usize,
        initial_capacity: usize,
    ) -> Result<(Arc<Self>, Arc<ArrayQueue<ObjRawKey>>), Error> {
        let pool = Self::new(path, layout, capacity, obj_size)?;
        pool.set_initial_capacity(obj_size, data_type, capacity, initial_capacity)
    }

    pub fn pair_allocator(
        pool: &Arc<Self>,
        obj_size: usize,
        data_type: u64,
        capacity: usize,
    ) -> ObjAllocator {
        ObjAllocator {
            pool: Arc::clone(pool),
            obj_size,
            data_type,
            capacity,
            allocated: 0,
        }
    }

    pub fn with_allocator<P: AsRef<Path>, S: Into<String>>(
        path: P,
        layout: Option<S>,
        obj_size: usize,
        data_type: u64,
        capacity: usize,
    ) -> Result<(Arc<Self>, ObjAllocator), Error> {
        let pool = Arc::new(Self::new(path, layout, obj_size, capacity)?);
        let allocator = Self::pair_allocator(&pool, obj_size, data_type, capacity);
        Ok((pool, allocator))
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
        #[allow(clippy::identity_conversion)]
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

    pub fn remove(&self, key: ObjRawKey) -> Result<(), Error> {
        let mut oid = PMEMoid::from(key);
        if self.uuid_lo != oid.pool_uuid_lo() {
            return Err(ErrorKind::PmdkKeyNotBelongToPool.into());
        }
        unsafe { pmemobj_free(&mut oid as *mut PMEMoid) };
        Ok(())
    }

    /// # Safety
    /// Should be called on valid ObjRawKey only
    pub unsafe fn get_by_rawkey(&self, rkey: ObjRawKey, buf: &mut [u8]) -> ObjRawKey {
        buf.copy_from_slice(std::slice::from_raw_parts(
            rkey.as_ptr() as *const u8,
            buf.len(),
        ));
        rkey
    }

    pub fn set_rm_on_drop(&mut self, rm_pool: bool) {
        self.rm_on_drop = rm_pool;
    }

    pub fn iter(&self) -> ObjPoolIter {
        unsafe { pmemobj_first(self.inner) }.into()
    }

    /// Create and set arena for current thread
    /// # Safety
    /// Working with raw pointers potentially unsafe
    pub unsafe fn thread_arena_init(&self) -> Result<(), Error> {
        let mut arena_id: u32 = 0;
        let name = str_as_c_char("heap.arena.create")?;
        let _ret = pmemobj_ctl_exec(self.inner, name, &mut arena_id as *mut u32 as *mut c_void);
        let name = str_as_c_char("heap.thread.arena_id")?;
        let _ret = pmemobj_ctl_set(self.inner, name, &mut arena_id as *mut u32 as *mut c_void);
        Ok(())
    }

    /// Get heap arena id
    /// # Safety
    /// Working with raw pointers potentially unsafe
    pub unsafe fn thread_arena_get(&self) -> Result<u32, Error> {
        let mut arena_id: u32 = 0;
        let name = str_as_c_char("heap.thread.arena_id")?;
        let _ret = pmemobj_ctl_get(self.inner, name, &mut arena_id as *mut u32 as *mut c_void);
        Ok(arena_id)
    }
}

impl Drop for ObjPool {
    fn drop(&mut self) {
        // TODO: remove for debug only
        println!("Dropping obj pool {:?}", self.inner);
        unsafe {
            pmemobj_close(self.inner);
            if self.rm_on_drop {
                pmempool_rm(self.inner_path.as_ptr(), 0);
            }
        }
    }
}

#[derive(Debug)]
pub struct ObjPoolIter(PMEMoid);

unsafe impl Send for ObjPoolIter {}

impl From<PMEMoid> for ObjPoolIter {
    fn from(oid: PMEMoid) -> Self {
        Self(oid)
    }
}

impl Iterator for ObjPoolIter {
    type Item = ObjRawKey;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if !self.0.is_null() {
            let current = self.0;
            self.0 = unsafe { pmemobj_next(current) };
            Some(current.into())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::mem;
    use std::sync::Arc;

    use core::ops::Deref;
    use crossbeam_queue::ArrayQueue;
    use futures::future::Future;
    use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};
    #[allow(unused_imports)]
    use test::Bencher;
    use tokio::run;

    use crate::error::{Kind as ErrorKind, WrapErr};
    use crate::{Error, ObjAllocator, ObjRawKey};
    use std::path::PathBuf;

    const DIRECT_PTR_MASK: u64 = !0u64 >> ((mem::size_of::<u64>() * 8 - 4) as u64);

    const TEST_TYPE_NUM: u64 = 0xf;

    struct TmpPool {
        inner: Arc<ObjPool>,
        #[allow(dead_code)]
        dir: tempfile::TempDir,
    }

    impl Deref for TmpPool {
        type Target = Arc<ObjPool>;

        #[inline]
        fn deref(&self) -> &Arc<ObjPool> {
            &self.inner
        }
    }

    impl TmpPool {
        fn prepare(name: &str) -> Result<(tempfile::TempDir, PathBuf), Error> {
            let dir = tempfile::tempdir().map_err(|_| ErrorKind::GenericError)?;
            let path = dir.path().join(name);
            Ok((dir, path))
        }

        fn new_with_size(name: &str, size: usize) -> Result<Self, Error> {
            let (dir, path) = Self::prepare(name)?;
            ObjPool::with_size::<_, String>(path, None, size)
                .map(Arc::new)
                .map(|inner| Self { inner, dir })
        }

        fn new(name: &str, obj_size: usize, capacity: usize) -> Result<Self, Error> {
            let (dir, path) = Self::prepare(name)?;
            ObjPool::new::<_, String>(path, None, obj_size, capacity)
                .map(Arc::new)
                .map(|inner| Self { inner, dir })
        }

        fn new_with_capacity(
            name: &str,
            obj_size: usize,
            capacity: usize,
        ) -> Result<(Self, ArrayQueue<ObjRawKey>), Error> {
            let (dir, path) = Self::prepare(name)?;
            ObjPool::with_capacity::<_, String>(path, None, obj_size, TEST_TYPE_NUM, capacity)
                .map(|(pool, aqueue)| (Arc::new(pool), aqueue))
                .map(|(inner, aqueue)| (Self { inner, dir }, aqueue))
        }

        fn new_with_capacity_differed(
            name: &str,
            obj_size: usize,
            capacity: usize,
            initial_capacity: usize,
        ) -> Result<(Self, Arc<ArrayQueue<ObjRawKey>>), Error> {
            let (dir, path) = Self::prepare(name)?;
            ObjPool::with_initial_capacity::<_, String>(
                path,
                None,
                obj_size,
                TEST_TYPE_NUM,
                capacity,
                initial_capacity,
            )
            .map(|(inner, aqueue)| (Self { inner, dir }, aqueue))
        }

        fn new_with_allocator(
            name: &str,
            obj_size: usize,
            capacity: usize,
        ) -> Result<(Self, ObjAllocator), Error> {
            let (dir, path) = Self::prepare(name)?;
            ObjPool::with_allocator::<_, String>(path, None, obj_size, TEST_TYPE_NUM, capacity)
                .map(|(inner, allocator)| (Self { inner, dir }, allocator))
        }
    }

    fn verify_objs(
        obj_pool: &ObjPool,
        keys_vals: Vec<(ObjRawKey, Vec<u8>, u64)>,
    ) -> Result<Vec<(ObjRawKey, Vec<u8>, u64)>, Error> {
        keys_vals
            .into_iter()
            .map(|(rkey, val, val_type_num)| {
                let mut buf = Vec::with_capacity(val.len());
                let key = unsafe {
                    buf.set_len(val.len());
                    obj_pool.get_by_rawkey(rkey, &mut buf)
                };
                if buf == val && key.get_type() == val_type_num {
                    Ok((key, val, val_type_num))
                } else {
                    Err(ErrorKind::GenericError.into())
                }
            })
            .collect::<Result<Vec<(ObjRawKey, Vec<u8>, u64)>, Error>>()
    }

    fn iter_verify_objs(
        obj_pool: &ObjPool,
        keys_vals: Vec<(ObjRawKey, Vec<u8>, u64)>,
    ) -> Result<Vec<(ObjRawKey, Vec<u8>, u64)>, Error> {
        let mut hmap = HashMap::new();
        for (key, val, val_type_num) in keys_vals {
            hmap.insert(key, (val, val_type_num));
        }
        let iter = obj_pool.iter();
        iter.map(|key| {
            hmap.remove(&key)
                .map(|(val, val_type_num)| (key, val, val_type_num))
                .ok_or_else(|| ErrorKind::GenericError.into())
        })
        .collect::<Result<Vec<(ObjRawKey, Vec<u8>, u64)>, Error>>()
    }

    #[test]
    fn create() -> Result<(), Error> {
        let obj_size = 0x1000; // 4k
        let size = 0x100_0000; // 16 Mb
        let obj_pool = TmpPool::new("__pmdk_basic__create_test.obj", obj_size, size / obj_size)?;
        println!("create:: MEM pool create: done!");

        let keys_vals = (0..100)
            .map(|i| {
                let buf = vec![0xafu8; 0x10]; // 16 byte
                obj_pool.put(&buf, i as u64)
                    .map(u64::from)
                    .map(|key| {
                        if key & DIRECT_PTR_MASK != 0u64 {
                            println!(
                                "create:: verification error key 0x{:x} bx{:b} mask 0x{:b} result 0x{:b}",
                                key,
                                key,
                                DIRECT_PTR_MASK,
                                key & DIRECT_PTR_MASK
                            );
                        }
                        (key.into(), buf, i)
                    })
            })
            .collect::<Result<Vec<(ObjRawKey, Vec<u8>, u64)>, Error>>()?;
        println!("create:: MEM put: done!");

        verify_objs(&obj_pool, keys_vals).map(|_| ())
    }

    fn wait_for_capacity(
        aqueue: &ArrayQueue<ObjRawKey>,
        capacity: usize,
        interval: u64,
        name: &str,
    ) {
        loop {
            let cur_capacity = aqueue.len();
            if cur_capacity < capacity {
                println!(
                    "{}: objects allocated/capacity {}/{}  -> sleep({})",
                    name, cur_capacity, capacity, interval,
                );
                std::thread::sleep(std::time::Duration::from_secs(interval));
            } else {
                println!(
                    "{}: objects allocated/capacity {}/{}  -> full capacity reached",
                    name, cur_capacity, capacity,
                );
                break;
            }
        }
    }

    fn update_buf_head(buf: &mut [u8], key: &ObjRawKey) {
        let key_bytes = key.as_byte_vec();
        println!("{:?} key:{:x?}", key, key_bytes);
        buf[..key_bytes.len()].copy_from_slice(key_bytes.as_slice());
    }

    fn generate_objs(
        obj_pool: Arc<ObjPool>,
        aqueue: Arc<ArrayQueue<ObjRawKey>>,
        nobj: usize,
    ) -> Result<Vec<(ObjRawKey, Vec<u8>, u64)>, Error> {
        (0..nobj)
            .map(|_| {
                let mut buf = vec![0xafu8; 0x1000]; // 4k
                let key = aqueue.pop().wrap_err(ErrorKind::GenericError)?;
                update_buf_head(&mut buf, &key);
                let key = obj_pool.update_by_rawkey(key, &buf, None)?;
                Ok((key, buf, TEST_TYPE_NUM))
            })
            .collect::<Result<Vec<(ObjRawKey, Vec<u8>, u64)>, Error>>()
    }

    fn update_objs(
        obj_pool: Arc<ObjPool>,
        aqueue: Arc<ArrayQueue<ObjRawKey>>,
        nobj: usize,
        name: &str,
    ) -> Result<(), Error> {
        let keys_vals = generate_objs(Arc::clone(&obj_pool), aqueue, nobj)?;
        println!("{}:: MEM put: done!", name);

        let keys_vals = verify_objs(&obj_pool, keys_vals)?;
        println!("{}:: MEM get: done!", name);

        let keys_vals = keys_vals
            .into_iter()
            .map(|(key, _, type_num)| {
                let mut buf1 = vec![0xafu8; 0x800]; // 2k
                update_buf_head(&mut buf1, &key);
                let mut buf2 = vec![0xbcu8; 0x800]; // 2k
                let key = obj_pool.update_by_rawkey(key, &buf2, buf1.len())?;
                buf1.append(&mut buf2);
                Ok((key, buf1, type_num))
            })
            .collect::<Result<Vec<(ObjRawKey, Vec<u8>, u64)>, Error>>()?;
        println!("{}:: MEM put partial: done!", name);

        verify_objs(&obj_pool, keys_vals)?;
        println!("{}:: MEM get partial: done!", name);
        Ok(())
    }

    #[test]
    fn preallocate() -> Result<(), Error> {
        let (obj_pool, aqueue) =
            TmpPool::new_with_capacity("__pmdk_basic__preallocate_test.obj", 0x1000, 0x800)?;

        println!("preallocate:: allocated {} objects", aqueue.len());

        let aqueue = Arc::new(aqueue);

        update_objs(
            Arc::clone(&obj_pool),
            Arc::clone(&aqueue),
            10,
            "preallocate",
        )
    }

    #[test]
    fn verify_iter() -> Result<(), Error> {
        let capacity = 0x800;
        let (obj_pool, aqueue) =
            TmpPool::new_with_capacity("__pmdk_basic__verifyiter_test.obj", 0x1000, capacity)?;

        println!("verify_iter:: allocated {} objects", aqueue.len());

        let aqueue = Arc::new(aqueue);
        let keys_vals = generate_objs(Arc::clone(&obj_pool), Arc::clone(&aqueue), capacity)?;
        let keys_vals = iter_verify_objs(&obj_pool, keys_vals)?;
        if keys_vals.len() == capacity {
            verify_objs(&obj_pool, keys_vals).map(|_| ())
        } else {
            Err(ErrorKind::GenericError.into())
        }
    }

    #[test]
    fn preallocate_differed() -> Result<(), Error> {
        let name = "preallocate differed";
        let mut capacity = 0x800;
        let (obj_pool, aqueue) = TmpPool::new_with_capacity_differed(
            "__pmdk_basic__preallocate_differed_test.obj",
            0x1000,
            capacity,
            0x100,
        )?;

        println!("preallocate differed:: allocated {} objects", aqueue.len());

        update_objs(Arc::clone(&obj_pool), Arc::clone(&aqueue), 10, name)?;
        capacity -= 10;

        update_objs(Arc::clone(&obj_pool), Arc::clone(&aqueue), 100, name)?;
        capacity -= 100;

        wait_for_capacity(&aqueue, capacity, 5, name);

        Ok(())
    }

    struct AlignedData {
        _f1: u64,
        _f2: u64,
    }

    #[test]
    fn alloc_alignment() -> Result<(), Error> {
        let obj_size = mem::size_of::<AlignedData>();
        let (_obj_pool, aqueue) = TmpPool::new_with_capacity(
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

    #[test]
    fn allocator() -> Result<(), Error> {
        let name = "allocator";
        let obj_size = 0x1000;
        let mut capacity = 0x800;
        let (obj_pool, mut allocator) =
            TmpPool::new_with_allocator("__pmdk_basic_allocator_test.obj", obj_size, capacity)?;

        let aqueue = Arc::new(ArrayQueue::new(capacity));
        let aqueue_clone = Arc::clone(&aqueue);
        let threads = CpuPool::new(10);

        let alloc_task = threads
            .spawn_fn(|| {
                let capacity = allocator.capacity();
                (0..capacity)
                    .map(move |_| {
                        allocator.allocate().and_then(|key| {
                            key.map(|key| {
                                aqueue_clone
                                    .push(key)
                                    .wrap_err(ErrorKind::PmdkNoSpaceInQueueError)
                            })
                            .unwrap_or_else(|| Err(ErrorKind::GenericError.into()))
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()
            })
            .map(|_| ())
            .map_err(|_| ());

        let allocation_context = std::thread::spawn(|| run(alloc_task));

        capacity -= 10;
        wait_for_capacity(&aqueue, 10, 5, name);
        update_objs(Arc::clone(&obj_pool), Arc::clone(&aqueue), 10, name)?;

        wait_for_capacity(&aqueue, capacity, 5, name);
        update_objs(Arc::clone(&obj_pool), Arc::clone(&aqueue), 100, name)?;

        allocation_context
            .join()
            .map_err(|_| ErrorKind::GenericError.into())
    }

    fn path_exists(path: &CStr) -> Result<(), Error> {
        nix::unistd::access(path, nix::unistd::AccessFlags::F_OK)
            .map_err(|_| ErrorKind::GenericError.into())
    }

    fn path_doesnt_exists(path: &CStr) -> Result<(), Error> {
        path_exists(path)
            .and(Err(ErrorKind::GenericError.into()))
            .or(Ok(()))
    }

    fn path_rm(path: &CStr) -> Result<(), Error> {
        nix::unistd::unlink(path).map_err(|_| ErrorKind::GenericError.into())
    }

    #[test]
    fn test_rm_on_drop() -> Result<(), Error> {
        let obj_size = 0x1000; // 4k
        let size = 0x100_0000; // 16 Mb
        let path = {
            let obj_pool = ObjPool::new::<_, String>(
                "__pmdk_basic__rm_on_drop.obj",
                None,
                obj_size,
                size / obj_size,
            )?;
            obj_pool.inner_path.clone()
        };

        path_exists(&path)?;
        path_rm(&path)?;
        path_doesnt_exists(&path)?;

        let path = {
            let mut obj_pool = ObjPool::new::<_, String>(
                "__pmdk_basic__rm_on_drop.obj",
                None,
                obj_size,
                size / obj_size,
            )?;
            obj_pool.set_rm_on_drop(true);
            obj_pool.inner_path.clone()
        };

        path_doesnt_exists(&path)
    }

    // sizes in kbytes
    const RANDOM_DATA_SIZE: usize = 1024 * 1024;
    static IO_SIZES: &[usize] = &[1024 * 1024, 128 * 1024, 64 * 1024, 32 * 1024, 16 * 1024];

    fn var_alloc_task(pool: Arc<ObjPool>, io_sizes_start: usize, nobj: usize) -> Result<(), Error> {
        use rand::Rng;
        let arena_id = unsafe { pool.thread_arena_get() }?;
        let io_sizes: &'static [usize] = &IO_SIZES[io_sizes_start..];

        let mut rng = rand::thread_rng();
        let random_data = (0..RANDOM_DATA_SIZE)
            .map(|_| rng.gen_range(0, std::u8::MAX))
            .collect::<Vec<u8>>();
        let keys = (0..nobj)
            .map(|i| {
                let size = io_sizes[(i + arena_id as usize) % io_sizes.len()];
                pool.put(&random_data[0..size], 0)
            })
            .collect::<Result<Vec<ObjRawKey>, Error>>()?;
        keys.into_iter()
            .map(|key| pool.remove(key))
            .collect::<Result<Vec<()>, Error>>()
            .map(|_| ())
    }

    fn var_alloc_prepare(size: usize, nthreads: usize) -> Result<(TmpPool, CpuPool), Error> {
        let obj_pool = TmpPool::new_with_size("__pmdk_var_alloc_basic_test.obj", size)?;

        let obj_pool_clone = obj_pool.clone();
        let threads = CpuPoolBuilder::new()
            .pool_size(nthreads)
            .after_start(move || unsafe {
                obj_pool_clone
                    .clone()
                    .thread_arena_init()
                    .expect("PMEM thread var size allocator creation failed!");
            })
            .create();
        Ok((obj_pool, threads))
    }

    fn var_alloc_run(
        obj_pool: Arc<ObjPool>,
        threads: CpuPool,
        nthreads: usize,
        nobj: usize,
        io_sizes_start: usize,
    ) -> Result<(), Error> {
        futures::future::join_all((0..nthreads).map(move |_| {
            let obj_pool_for_task = obj_pool.clone();
            threads.spawn_fn(move || {
                var_alloc_task(Arc::clone(&obj_pool_for_task), io_sizes_start, nobj)
            })
        }))
        .wait()
        .map(|_| ())
    }

    #[test]
    fn var_alloc_basic() -> Result<(), Error> {
        let size = 0xa0_000_000;
        let nthreads = 10;
        let nobj = 1000;
        let (
            TmpPool {
                inner: pool,
                dir: _tmp_dir,
            },
            threads,
        ) = var_alloc_prepare(size, nthreads)?;
        var_alloc_run(Arc::clone(&pool), threads, nthreads, nobj, 1)
    }

    #[bench]
    fn bench_var_alloc(b: &mut Bencher) {
        let size = 0xa0_000_000;
        let nthreads = 10;
        let nobj = 10;
        let (
            TmpPool {
                inner: pool,
                dir: _tmp_dir,
            },
            threads,
        ) = var_alloc_prepare(size, nthreads).expect("bench var alloc prepare");
        b.iter(|| var_alloc_run(Arc::clone(&pool), threads.clone(), nthreads, nobj, 1));
    }
}
