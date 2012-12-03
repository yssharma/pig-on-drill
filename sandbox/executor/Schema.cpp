/*
 * Schema.cpp
 *
 *  Created on: Nov 19, 2012
 *      Author: tnachen
 */

#include "Schema.h"

void SchemaPacker::pack_array(size_t array_length, const char * key, size_t key_length) {
	pack_key(key, key_length);
	msgpack_pack_int(schema_packer, drill_t_array);
	msgpack_pack_int(schema_packer, array_length);
}
void SchemaPacker::pack_map(size_t keys_length, const char * key, size_t key_length) {
	pack_key(key, key_length);
	msgpack_pack_int(schema_packer, drill_t_map);
	msgpack_pack_int(schema_packer, key_length);
}
void SchemaPacker::pack_string(const char * key, size_t key_length) {
	pack_key(key, key_length);
	msgpack_pack_int(schema_packer, drill_t_string);
}
void SchemaPacker::pack_int(const char * key, size_t key_length) {
	pack_key(key, key_length);
	msgpack_pack_int(schema_packer, drill_t_int);
}
void SchemaPacker::pack_double(const char * key, size_t key_length) {
	pack_key(key, key_length);
	msgpack_pack_int(schema_packer, drill_t_double);
}
void SchemaPacker::pack_boolean(const char * key, size_t key_length) {
	pack_key(key, key_length);
	msgpack_pack_int(schema_packer, drill_t_boolean);
}
void SchemaPacker::pack_unknown(const char * key, size_t key_length) {
	pack_key(key, key_length);
	msgpack_pack_int(schema_packer, drill_t_unknown);
}

void SchemaPacker::pack_key(const char * key, size_t key_length) {
	if (key && key_length > 0) {
		(*(schema_packer)->callback)((schema_packer)->data, (const char*)key, key_length);
	}
}

