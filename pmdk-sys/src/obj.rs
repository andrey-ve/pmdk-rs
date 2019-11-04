//
// Copyright (c) 2019 RepliXio Ltd. All rights reserved.
// Use is subject to license terms.
//

use libc::{c_char, c_int, c_void};
use libc::{mode_t, size_t};

use crate::PMEMoid;

pub enum PMEMobjpool {}

#[allow(non_camel_case_types)]
pub type pmemobj_constr =
    unsafe extern "C" fn(pop: *mut PMEMobjpool, ptr: *mut c_void, arg: *mut c_void) -> c_int;

#[link(name = "pmemobj", kind = "static")]
extern "C" {
    pub fn pmemobj_open(path: *const c_char, layout: *const c_char) -> *mut PMEMobjpool;
    pub fn pmemobj_create(
        path: *const c_char,
        layout: *const c_char,
        poolsize: size_t,
        mode: mode_t,
    ) -> *mut PMEMobjpool;
    pub fn pmemobj_close(pop: *mut PMEMobjpool);

    // Object

    pub fn pmemobj_alloc(
        pop: *mut PMEMobjpool,
        oidp: *mut PMEMoid,
        size: size_t,
        type_num: u64,
        constructor: Option<pmemobj_constr>,
        arg: *mut c_void,
    ) -> c_int;
    pub fn pmemobj_free(oidp: *mut PMEMoid);

    pub fn pmemobj_memcpy_persist(
        pop: *mut PMEMobjpool,
        dest: *mut c_void,
        src: *const c_void,
        len: size_t,
    );
    pub fn pmemobj_memset_persist(pop: *mut PMEMobjpool, dest: *mut c_void, c: c_int, len: size_t);
    pub fn pmemobj_persist(pop: *mut PMEMobjpool, addr: *const c_void, len: size_t);
    pub fn pmemobj_flush(pop: *mut PMEMobjpool, addr: *const c_void, len: size_t);
    pub fn pmemobj_drain(pop: *mut PMEMobjpool);

    // Error handling:

    pub fn pmemobj_errormsg() -> *const c_char;

    pub fn pmemobj_direct(oid: PMEMoid) -> *mut c_void;

    pub fn pmemobj_oid(addr: *const c_void) -> PMEMoid;

    pub fn pmemobj_type_num(oid: PMEMoid) -> u64;

    pub fn pmemobj_first(pop: *const PMEMobjpool) -> PMEMoid;
    pub fn pmemobj_next(oid: PMEMoid) -> PMEMoid;
}
