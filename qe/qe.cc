
#include "qe.h"

// Filter
Filter::Filter(Iterator* input, const Condition &condition) : itr(input), cond(condition) {
	TableScan * t_ptr = dynamic_cast<TableScan *>(itr);
	t_ptr->setIterator();
}

RC Filter::getNextTuple(void *data) {
	//cast the iterator to a tablescan type
	TableScan * t_ptr = dynamic_cast<TableScan *>(itr);
	if (!t_ptr){
		cout<< "bad cast\n";
		return INVALID_TABLE_OBJECT;
	}
    string tableName  = parseTableName(cond.lhsAttr);
    string atrributeName = parseAttributeName(cond.lhsAttr);
	unsigned attributePosition = getConditionTarget(t_ptr->attrs, atrributeName);
	unsigned rhAttributePosition;
	if (attributePosition == QE_INVALID_ATTRIBUTE){
	//	cout<<"bad position\n";
		return attributePosition;
	}
    //keep running sizes of the attributes
	unsigned leftSize =0;
	unsigned rightSize = 0;
	int nullIndicatorSize = int(ceil((double)t_ptr->attrs.size() / CHAR_BIT));
	int counter =0;
	bool valid;
	while (t_ptr->getNextTuple(data) != RM_EOF){
        //accuire left hand variable
		void * leftHand;
		leftSize = getAttSize(t_ptr->attrs, attributePosition, data);
		leftHand = malloc(leftSize);
        getAttributeData(t_ptr->attrs, attributePosition, leftSize, data, leftHand);
//        cout<< "got left hand: "<< ((int*)leftHand)[0]<<endl;
		//accuire right hand variable
        void * rightHand;
        if(cond.bRhsIsAttr){
            cout<<"has right\n";
            rhAttributePosition = getConditionTarget(t_ptr->attrs, cond.rhsAttr);
			rightSize = getAttSize(t_ptr->attrs, rhAttributePosition, data);
			rightHand = malloc(rightSize);
            getAttributeData(t_ptr->attrs,rhAttributePosition, rightSize, data, rightHand);
        }
        else{	//get stored value
//			cout<< "have to fetch stored value\n";
			switch(cond.rhsValue.type){
				case TypeInt:
				{
					rightHand = malloc(INT_SIZE);
					rightSize = INT_SIZE;
					memcpy(rightHand,cond.rhsValue.data, rightSize);
					break;
				}
				case TypeReal:
				{
					rightHand = malloc(REAL_SIZE);
					rightSize = REAL_SIZE;
					memcpy(rightHand,cond.rhsValue.data, rightSize);
					break;
				}
				case TypeVarChar:
				{
					rightSize = ((int *)cond.rhsValue.data)[0];
					rightHand = malloc(rightSize + INT_SIZE);
					memcpy(rightHand,&rightSize, INT_SIZE);
					memcpy(rightHand,cond.rhsValue.data + INT_SIZE, rightSize);
					break;
				}
			}
			
        }
//		cout<< "right: "<< ((int*)rightHand)[0]<<endl;
		//if comparison holds return it
		int compareVal = attCompare(leftHand, rightHand, t_ptr->attrs[attributePosition].type);
		if (validCompare(compareVal, cond.op)){
//			cout<< "meets comparison check\n";
			free(rightHand);
			free(leftHand);
			return SUCCESS;
		}
		else{
			cout<< "does not meet comparison check\n";
			free(rightHand);
			free(leftHand);
		}
		//cout<< ++counter<<endl;
	}
	//reached end of 
	return QE_EOF;
}
//used to get the size to malloc
unsigned Filter::getAttSize(vector<Attribute> &attrs, unsigned attrPos, void * data){
	unsigned size;
	switch(attrs[attrPos].type){
		case TypeInt:
			size = INT_SIZE;
			break;
		case TypeReal:
			size = REAL_SIZE;
			break;
		case TypeVarChar:
			size = attrs[attrPos].length + INT_SIZE;
			break;
	}
	return size;
}
//used to break up the . for table name
string Filter::parseAttributeName(const string name){
    string dot = ".";
    size_t pos = name.find(dot);
    string out;
    out.insert(0, name, pos +1, name.size() - (pos+ 1));
//    cout<< "name: "<<out<<endl;
    return out;
}
//used to break up the . for attribute name
string Filter::parseTableName(const string name){
    string dot = ".";
    size_t pos = name.find(dot);
    string out;
    out.insert(0, name, 0, pos);
//    cout<< "table: "<<out<<endl;
    return out;
}

//sees if the return value is valid for the comparison
bool Filter::validCompare(int returnValue, CompOp compOp){
//	cout<< "return value: "<< returnValue<<endl;
	if (compOp == NO_OP)
		return true;
	if (!returnValue){
		if(compOp == EQ_OP ||compOp == LE_OP ||compOp == GE_OP){
			return true;
		}
	}
	if (returnValue != 0){
		if(compOp == NE_OP){
			return true;
		}
	}
	if(returnValue > 0 && compOp == GT_OP || returnValue > 0 && compOp == GE_OP)
		return true;
	if (returnValue < 0 && compOp == LT_OP || returnValue < 0 && compOp == LE_OP)
		return true;
	return false;
}
//funnels to the appropiate comparison
int Filter::attCompare(void * left, void * right, AttrType type){
	switch(type){
		case TypeInt:
		{
			int leftInt = ((int*)left)[0];
			int rightInt = ((int*)right)[0];
			return compare(leftInt,rightInt);
			break;
		}
		case TypeReal:
		{
			float leftFloat = ((float*)left)[0];
			float rightFloat = ((float*)right)[0];
			return compare(leftFloat,rightFloat);
			break;
		}
		case TypeVarChar:
		{
			char terminatingNull = '/0';
			int leftSize = ((int*)left)[0];
			int rightSize = ((int*)right)[0];
			char cast1[leftSize + 1];
			char cast2[leftSize + 1];
			memcpy(&cast1, left + INT_SIZE, leftSize);
			memcpy(&cast2, right + INT_SIZE, rightSize);
			cast1[leftSize] = '/0';
			cast2[rightSize] = '/0';
			return compare(cast1, cast2);
			break;
		}
	}
}
//integer compare
int Filter::compare(const int key, const int value) const
{
    if (key == value)
        return 0;
    else if (key > value)
        return 1;
    return -1;
}
//reals compare
int Filter::compare(const float key, const float value) const
{
    if (key == value)
        return 0;
    else if (key > value)
        return 1;
    return -1;
}
//varchar compare
int Filter::compare(const char *key, const char *value) const
{
    return strcmp(key, value);
}
//sets the built in value from condition
void Filter::getDataFromValue(Condition cond, void * output){
	unsigned size;
	switch(cond.rhsValue.type){
		case TypeInt:
		{
			output = malloc(INT_SIZE);
			size = INT_SIZE;
			memcpy(output,cond.rhsValue.data, size);
			break;
		}
		case TypeReal:
		{
			output = malloc(REAL_SIZE);
			size = REAL_SIZE;
			memcpy(output,cond.rhsValue.data, size);
			break;
		}
		case TypeVarChar:
		{
			size = ((int *)cond.rhsValue.data)[0];
			output = malloc(size + INT_SIZE);
			memcpy(output,&size, INT_SIZE);
			memcpy(output,cond.rhsValue.data + INT_SIZE, size);
			break;
		}
	}
}

//assist in geting position of an atribute in a vector
unsigned Filter::getConditionTarget(vector<Attribute> &attrs, string target){
//    cout<< "vector size: "<<attrs.size()<<endl;
//    cout<< "looking for "<<target<<endl;
	for (unsigned i =0; i < attrs.size(); i++){
//        cout<< "current name : "<< attrs[i].name<<endl;
		if(attrs[i].name == target)
			return i;
	}
	return QE_INVALID_ATTRIBUTE;
}
//used to allocate and fetch the attribute
void Filter::getAttributeData(vector<Attribute> &attrs, unsigned attrPos, unsigned size, void * data, void * output){
	char * cast = (char *)data;
	int nullBytes = int(ceil((double)attrs.size() / CHAR_BIT));
//    cout<< "null bytes: "<<nullBytes<< "att pos: "<<attrPos<<endl;
	cast += nullBytes;
	for (unsigned i =0; i < attrPos-1; i ++){
		//NOTE: need to check for nulls
		switch(attrs[i].type){
			case TypeInt:
				cast += INT_SIZE;
				break;
			case TypeReal:
				cast += REAL_SIZE;
				break;
			case TypeVarChar:
				int size = ((int*)cast)[0];
				cast += INT_SIZE;
				cast += size;
				break;
		}
	}
//    cout<< "cast moved\n";
	//cast is at the attribute now copy it
	memcpy(output, cast, size);
//    cout<< "left: "<< ((int*)output)[0]<<endl;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	TableScan * t_ptr = dynamic_cast<TableScan *>(itr);
	if (!t_ptr)
		return;
	cout<< "casted properly\n";
	attrs = t_ptr->attrs;
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
