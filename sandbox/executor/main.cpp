//============================================================================
// Name        : drill_execution.cpp
// Author      : Timothy Chen
// Version     :
// Copyright   : Your copyright notice
//============================================================================

#include <iostream>
#include "ScanJson.h"
#include "Filter.h"
using namespace std;

int main() {
	ScanJson sc("test.json");
	LessThanPredicate pred(2);
	while(sc.has_next()) {
		if(pred.is_matched(sc.next())) {
			cout << "Matched!!!!!";
		}
	}
	return 0;
}
