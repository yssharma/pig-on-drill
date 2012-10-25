/*
 * ScanJson.cpp
 *
 */

#include "ScanJson.h"
#include <iostream>
#include <fstream>
#include <string.h>

ScanJson::ScanJson(const char * source) :
		row_length(0), cur_idx(0) {
	char errbuf[1024];
	yajl_val node = read_json(source, errbuf);
	pack_object = new msgpack_object;
	if (node != NULL) {
		msgpack_zone mempool;
		msgpack_zone_init(&mempool, 2048);
		msgpack_sbuffer sbuf;
		msgpack_sbuffer_init(&sbuf);
		msgpack_packer pk;
		msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
		to_msgpack(node, &pk);
		msgpack_unpack(sbuf.data, sbuf.size, NULL, &mempool, pack_object);

		// Hardcoding to expect a map since the root object that's valid for JSON is a map
		pack_object = &pack_object->via.map.ptr->val;

		if (pack_object->type == MSGPACK_OBJECT_ARRAY) {
			row_length = pack_object->via.array.size;
			next_object = pack_object->via.array.ptr;
		} else {
			row_length = 1;
			next_object = pack_object;
		}

		msgpack_sbuffer_destroy(&sbuf);
		msgpack_zone_destroy(&mempool);
		yajl_tree_free(node);
	} else {
		cout << errbuf;
	}
}

bool ScanJson::has_next() {
	return cur_idx < row_length;
}

msgpack_object * ScanJson::next() {
	msgpack_object * return_object = NULL;
	if (next_object) {
		return_object = next_object;
		cur_idx += 1;
		if (has_next()) {
			next_object = pack_object->via.array.ptr + cur_idx;
		}
	}
	return return_object;
}

void ScanJson::to_msgpack(yajl_val curNode, msgpack_packer * packer) {
	switch (curNode->type) {
	case yajl_t_array: {
		unsigned int length = YAJL_GET_ARRAY(curNode)->len;
		yajl_val * array_values = YAJL_GET_ARRAY(curNode)->values;
		msgpack_pack_array(packer, length);
		for (unsigned int i = 0; i < length; ++i) {
			to_msgpack(*array_values, packer);
			array_values += 1;
		}
	}
		break;

	case yajl_t_string: {
		const char * string = YAJL_GET_STRING(curNode);
		size_t string_length = strlen(string);
		msgpack_pack_raw(packer, string_length);
		msgpack_pack_raw_body(packer, string, string_length);
	}
		break;
	case yajl_t_number: {
		if (curNode->u.number.flags & YAJL_NUMBER_DOUBLE_VALID) {
			msgpack_pack_double(packer, YAJL_GET_DOUBLE(curNode));
		} else {
			msgpack_pack_long_long(packer, YAJL_GET_INTEGER(curNode));
		}
	}
		break;
	case yajl_t_object: {
		unsigned int obj_length = YAJL_GET_OBJECT(curNode)->len;
		const char ** keys = YAJL_GET_OBJECT(curNode)->keys;

		yajl_val * values = YAJL_GET_OBJECT(curNode)->values;
		msgpack_pack_map(packer, obj_length);
		for (unsigned int i = 0; i < obj_length; ++i) {
			const char * cur_key = *(keys + i);
			size_t string_length = strlen(cur_key);
			msgpack_pack_raw(packer, string_length);
			msgpack_pack_raw_body(packer, cur_key, string_length);
			to_msgpack(*(values + i), packer);
		}
	}
		break;
	case yajl_t_true:
		msgpack_pack_true(packer);
		break;
	case yajl_t_false:
		msgpack_pack_false(packer);
		break;
	case yajl_t_null:
		msgpack_pack_nil(packer);
		break;
	default:
		break;
	}
}

yajl_val ScanJson::read_json(const char * source, char * error) {
	std::string data;
	std::ifstream in;
	char buffer[4096];

	in.open(source, ios::binary);
	while (in.read(buffer, sizeof(buffer))) {
		data.append(buffer, sizeof(buffer));
	}
	data.append(buffer, in.gcount());

	yajl_val rootNode = yajl_tree_parse((const char *) data.c_str(), error,
			sizeof(error));

	in.close();

	return rootNode;
}

ScanJson::~ScanJson() {
}

