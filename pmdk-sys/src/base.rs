/*
 * TODO: add header
 */
use std::fmt;


#[repr(C)]
#[derive(Copy, Clone)]
pub struct pmemoid
{
    pub pool_uuid_lo: u64,
    pub off: u64,
}

impl Default for pmemoid
{
    #[inline(always)]
    fn default() -> Self
    {
        Self { pool_uuid_lo: 0, off: 0}
    }
}

impl fmt::Debug for pmemoid
{
    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
    {
        write!(f, "pmemoid pool: {}, off: {:x}", self.pool_uuid_lo, self.off)
    }
}

impl pmemoid {
    pub fn off(&self) -> u64 {
        self.off
    }

    pub fn pool_uuid_lo(&self) -> u64 {
        self.pool_uuid_lo
    }
}

pub type PMEMoid = pmemoid;

