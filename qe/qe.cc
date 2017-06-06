
#include "qe.h"

// Filter
Filter::Filter(Iterator* input, const Condition &condition) : itr(input), cond(condition) {
}

RC Filter::getNextTuple(void *data) {
  return -1;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
}

// Project
Project::Project(Iterator *input, const vector<string> &attrNames) : itr(input), attributes(attrNames) {
}

RC Project::getNextTuple(void *data) {
  return -1;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
}


// INLJoin
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) : leftInput(leftIn), rightInput(rightIn), cond(condition) {
}

RC INLJoin::getNextTuple(void *data) {
  return -1;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
}
