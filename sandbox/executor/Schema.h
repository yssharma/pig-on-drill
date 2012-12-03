/*
 * Schema.h
 *
 *  Created on: Nov 16, 2012
 *      Author: tnachen
 */

#ifndef SCHEMA_H_
#define SCHEMA_H_

#include <msgpack.h>

typedef enum {
	drill_t_unknown = 0,
	drill_t_string = 1,
	drill_t_int = 2,
	drill_t_double = 3,
	drill_t_boolean = 4,
	drill_t_array = 5,
	drill_t_map = 6
} drill_type;

class SchemaPacker {
public:
	SchemaPacker() {
		schema_packer = new msgpack_packer;
		msgpack_sbuffer_init(schema_sbuf);
		msgpack_packer_init(schema_packer, &schema_sbuf,
		msgpack_sbuffer_write);
	};

	~SchemaPacker() {
		msgpack_sbuffer_destroy(schema_sbuf);
		delete schema_packer;
	}

	void pack_array(size_t array_length, const char * key = 0, size_t key_length = 0);
	void pack_map(size_t keys_length, const char * key = 0, size_t key_length = 0);
	void pack_string(const char * key = 0, size_t key_length = 0);
	void pack_int(const char * key = 0, size_t key_length = 0);
	void pack_double(const char * key = 0, size_t key_length = 0);
	void pack_boolean(const char * key = 0, size_t key_length = 0);
	void pack_unknown(const char * key = 0, size_t key_length = 0);

private:
	msgpack_sbuffer * schema_sbuf;
	msgpack_packer * schema_packer;
	void pack_key(const char * key, size_t key_length);
};

#endif /* SCHEMA_H_ */
