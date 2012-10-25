/*
 * Filter.h
 *
 *  Created on: Nov 7, 2012
 *      Author: tnachen
 */

#ifndef FILTER_H_
#define FILTER_H_

class LessThanPredicate {
public:
	LessThanPredicate(int value) :
			pred_value(value) {
	}

	bool is_matched(msgpack_object * value) {
		msgpack_object_type type = value->type;
		bool matched = false;
		switch (type) {
		case MSGPACK_OBJECT_POSITIVE_INTEGER:
			matched = value->via.u64 < pred_value;
			break;
		case MSGPACK_OBJECT_NEGATIVE_INTEGER:
			matched = value->via.i64 < pred_value;
			break;
		case MSGPACK_OBJECT_DOUBLE:
			matched = value->via.dec < pred_value;
			break;
		default:
			break;
		}
		return matched;
	}

private:
	int pred_value;
};

#endif /* FILTER_H_ */
