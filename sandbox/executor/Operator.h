
#ifndef DRILL_EXECUTOR_H
#define DRILL_EXECUTOR_H

#include <stdlib.h>
#include <stdio.h>

class Operator {

};

// Take no inputs and produce output
class ProduceOperator : public Operator {
	virtual void Produce();
	virtual ~ProduceOperator();
};

// Take inputs and produce output
class StreamOperator : public Operator {
	virtual void Process();
	virtual ~StreamOperator();
};

#endif
