/*
 * ScanJson.h
 *
 *  Created on: Oct 20, 2012
 *      Author: tnachen
 */

#ifndef DRILL_SCANJSON_H_
#define DRILL_SCANJSON_H_

#include "Operator.h"
#include <msgpack.h>
#include "Schema.h"
#include "yajl/yajl_tree.h"

using namespace std;

class ScanJson {
public:
	ScanJson(const char * source);
	~ScanJson();
	bool has_next();
	msgpack_object * next();
private:
	msgpack_object * pack_object;
	msgpack_object * next_object;
	int row_length;
	int cur_idx;
	yajl_val read_json(const char * source, char * error);
	void to_msgpack(yajl_val curNode, msgpack_packer * packer, SchemaPacker * schema_packer, const char * key = 0, size_t key_length = 0);
};


#endif /* SCANJSON_H_ */
