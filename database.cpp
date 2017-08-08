/*
Minetest
Copyright (C) 2013 celeron55, Perttu Ahola <celeron55@gmail.com>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

#include "database.h"
#include "irrlichttypes.h"
#include <iostream>
#include <chrono>

/****************
 * Black magic! *
 ****************
 * The position hashing is very messed up.
 * It's a lot more complicated than it looks.
 */

static inline s16 unsigned_to_signed(u16 i, u32 max_positive)
{
	if (i < max_positive) {
		return i;
	} else {
		return i - (max_positive * 2);
	}
}


// Modulo of a negative number does not work consistently in C
static inline s64 pythonmodulo(s64 i, s32 mod)
{
	if (i >= 0) {
		return i % mod;
	}
	return mod - ((-i) % mod);
}

s64 Database::getBlockAsInteger(const v3s16 &pos)
{
	return (u64) pos.Z * 0x1000000 +
		(u64) pos.Y * 0x1000 +
		(u64) pos.X;
}

int64_t Database::getBlockAsInteger(int16_t x, int16_t y, int16_t z)
{
    return (uint64_t)z * 0x1000000 +
    (uint64_t)y * 0x1000 +
    (uint64_t)x;
}


v3s16 Database::getIntegerAsBlock(s64 i)
{
	v3s16 pos;
	pos.X = unsigned_to_signed(pythonmodulo(i, 4096), 2048);
	i = (i - pos.X) / 4096;
	pos.Y = unsigned_to_signed(pythonmodulo(i, 4096), 2048);
	i = (i - pos.Y) / 4096;
	pos.Z = unsigned_to_signed(pythonmodulo(i, 4096), 2048);
	return pos;
}

bool Database::getIntegerAsBlock(int64_t i, int16_t& x, int16_t& y, int16_t& z)
{
    x = unsigned_to_signed(pythonmodulo(i, 4096), 2048);
    i = (i - x) / 4096;
    y = unsigned_to_signed(pythonmodulo(i, 4096), 2048);
    i = (i - y) / 4096;
    z = unsigned_to_signed(pythonmodulo(i, 4096), 2048);
    return true;
}

v3s16 Database::getIntegerAsNode(s64 i)
{
    v3s16 pos;
    pos.X = unsigned_to_signed(pythonmodulo(i, 65536), 32768);
    i = (i - pos.X) / 65536;
    pos.Y = unsigned_to_signed(pythonmodulo(i, 65536), 32768);
    i = (i - pos.Y) / 65536;
    pos.Z = unsigned_to_signed(pythonmodulo(i, 65536), 32768);
    return pos;
}

