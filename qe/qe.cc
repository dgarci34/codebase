
#include "qe.h"
#include <math.h>

// Filter
Filter::Filter(Iterator* input, const Condition &condition) : itr(input), cond(condition) {
}

RC Filter::getNextTuple(void *data) {
	//cast the iterator to a tablescan type
	TableScan * t_ptr = dynamic_cast<TableScan *>(itr);
	if (!t_ptr)
		return INVALID_TABLE_OBJECT;
	
	unsigned attributePosition = getConditionTarget(t_ptr->attrs, cond.lhsAttr);
    unsigned rhAttributePosition;
	if (attributePosition == QE_INVALID_ATTRIBUTE)
		return attributePosition;
	int nullIndicatorSize = int(ceil((double)t_ptr->attrs.size() / CHAR_BIT));
	while (t_ptr->getNextTuple(data) != QE_EOF){
        //accuire left hand variable
		void * leftHand;
        getAttributeData(t_ptr->attrs, attributePosition, data, leftHand);
		
		//accuire right hand variable
        void * rightHand;
        if(cond.bRhsIsAttr){
            rhAttributePosition = getConditionTarget(t_ptr->attrs, cond.rhsAttr);
            getAttributeData(t_ptr->attrs,rhAttributePosition,data,rightHand);
        }
        else{
            getDataFromValue(cond, rightHand);
        }
		
		//if comparison holds return it
		int compareVal = attCompare(leftHand, rightHand, t_ptr->attrs[attributePosition].type);
		if (validCompare(compareVal, cond.op)){
			free(rightHand);
			free(leftHand);
			return SUCCESS;
		}
		else{
			free(rightHand);
			free(leftHand);
		}
	}
	//reached end of 
	return QE_EOF;
}
//sees if the return value is valid for the comparison
bool Filter::validCompare(int returnValue, CompOp compOp){
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
	if(returnValue > 0 && compOp == GT_OP)
		return true;
	if (returnValue < 0 && compOp == LT_OP)
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
	for (unsigned i =0; i < attrs.size(); i++){
		if(attrs[i].name == target)
			return i;
	}
	return QE_INVALID_ATTRIBUTE;
}
//used to allocate and fetch the attribute
void Filter::getAttributeData(vector<Attribute> &attrs, unsigned attrPos, void * data, void * output){
	//allocate space
	unsigned size;
	switch(attrs[attrPos].type){
		case TypeInt:
			output = malloc(INT_SIZE);
			size = INT_SIZE;
			break;
		case TypeReal:
			output = malloc(REAL_SIZE);
			size = REAL_SIZE;
			break;
		case TypeVarChar:
			output = malloc(attrs[attrPos].length + INT_SIZE);
			size = attrs[attrPos].length + INT_SIZE;
			break;
	}
	output = malloc(size);
	char * cast = (char *)data;
	int nullBytes = int(ceil((double)attrs.size() / CHAR_BIT));
	cast += nullBytes;
	for (unsigned i =0; i < attrPos; i ++){
		//NOTE: need to check for nulls
		switch(attrs[i].type){
			case TypeInt:
				cast += INT_SIZE;
			case TypeReal:
				cast += REAL_SIZE;
			case TypeVarChar:
				int size = ((int*)cast)[0];
				cast += size;
		}
	}
	//cast is at the attribute now copy it
	memcpy(output, cast, size);
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

  //cast the iterator to a tablescan type
  TableScan * t_ptr = dynamic_cast<TableScan *>(itr);
  if (!t_ptr)
    return INVALID_TABLE_OBJECT;
  cout<< "casted properly\n";

  // declare variables
  void * tempData;
  uint32_t sizeOfData;
  uint32_t nullsIndicatorSize;
  unsigned char * nullsIndicator;
  uint32_t numOfData;
  vector<Attribute> tempDataAttrs = t_ptr->attrs;
  uint32_t offset;

  vector<string> projectedColumnNames = t_ptr->attrNames;
  uint32_t numOfProjectColumns;
  uint32_t projectOffset;
  uint32_t tempLength;
  uint32_t pcNullsIndicatorSize;
  unsigned char * pcNullsIndicator;

  // find the data size
  numOfData = tempDataAttrs.size();
  nullsIndicatorSize = ceil(numOfData/ 8);
  nullsIndicator = (unsigned char *) malloc(nullsIndicatorSize);
  sizeOfData = nullsIndicatorSize;

  for (uint32_t i = 0; i < numOfData; i++)
  {
    sizeOfData += tempDataAttrs[i].length;
  }
  tempData = malloc(sizeOfData);

  // get data
  if (t_ptr->getNextTuple(tempData))
  {
    // No more tuples
    free(nullsIndicator);
    free(tempData);
    return QE_EOF;
  }

  // set nulls indicator
  memcpy(nullsIndicator, tempData, nullsIndicatorSize);

  // find project columns
  numOfProjectColumns = projectedColumnNames.size();
  pcNullsIndicatorSize = ceil(numOfProjectColumns / 8);
  projectOffset = pcNullsIndicatorSize;
  pcNullsIndicator = (unsigned char *) malloc(pcNullsIndicatorSize);

  for (uint32_t j = 0; j < numOfProjectColumns; j++)
  {
    offset = nullsIndicatorSize;
    for (uint32_t k = 0; k < numOfData; k++)
    {
      // find the length of attribute
      if (fieldIsNull(nullsIndicator, k))
      {
        tempLength = 0;
      }
      else
      {
        switch (tempDataAttrs[k].type)
        {
          case TypeInt:
          {
            tempLength = sizeof(int);
            break;
          }
          case TypeReal:
          {
            tempLength = sizeof(float);
            break;
          }
          case TypeVarChar:
          {
            memcpy(&tempLength, ((char *)tempData + offset), sizeof(int));
            tempLength += sizeof(int);
            break;
          }
          default:
          {
            free(nullsIndicator);
            free(tempData);
            return QE_TYPE_ERROR;
          }
        }
      }

      // the project colum in original data
      if (projectedColumnNames[j].compare(tempDataAttrs[k].name) == 0)
      {
        if (tempLength != 0)
        {
          memcpy(((char *)data + projectOffset), ((char *)tempData + offset), tempLength);
          offset = tempLength;
        }
        else
        {
          setNullIndicator(pcNullsIndicator, j);
        }

        // exit the for loop
        k = numOfData;
      }

      offset += tempLength;
    }

    projectOffset += tempLength;
  }

  memcpy(data, pcNullsIndicator, pcNullsIndicatorSize);

  // free memory
  free(pcNullsIndicator);
  free(nullsIndicator);
  free(tempData);

  return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
  TableScan * t_ptr = dynamic_cast<TableScan *>(itr);
  if (!t_ptr)
    return;
  cout<< "casted properly\n";
  attrs = t_ptr->attrs;
}

bool Project::fieldIsNull(unsigned char *nullIndicator, int i) {
  int indicatorIndex = i / CHAR_BIT;
  int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
  return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

RC Project::setNullIndicator(unsigned char *nullIndicator, int i) {
  int indicatorIndex = i / 8;
  int positionInIndex = i % 8;
  switch (positionInIndex)
  {
    case 1:
      nullIndicator[indicatorIndex] ^ 10000000;
      break;
    case 2:
      nullIndicator[indicatorIndex] ^ 01000000;
      break;
    case 3:
      nullIndicator[indicatorIndex] ^ 00100000;
      break;
    case 4:
      nullIndicator[indicatorIndex] ^ 00010000;
      break;
    case 5:
      nullIndicator[indicatorIndex] ^ 00001000;
      break;
    case 6:
      nullIndicator[indicatorIndex] ^ 00000100;
      break;
    case 7:
      nullIndicator[indicatorIndex] ^ 00000010;
      break;
    case 0:
      nullIndicator[indicatorIndex] ^ 00000001;
      break;
    default:
      cout << "ERROR: positionInIndex is " << positionInIndex << endl;
      return -1;
  }
  return SUCCESS;
}
/*
void Project::setFirstBitToNull(char * byte)
{
  void * nullInt = calloc(1,1);
  int mask = 0b10000000;
  int pre = ((int *)byte)[0];
  memcpy(&mask, nullInt, 1);
  pre ^= mask;
  memcpy(byte, &pre, 1);
  free(nullInt);
}

void Project::setSecondBitToNull(char * byte)
{
  byte ^= 0b01000000;
}

void Project::setThirdBitToNull(char * byte)
{
  byte ^= 0b00100000;
}

void Project::setForthBitToNull(char * byte)
{
  byte ^= 0b00010000;
}

void Project::setFifthBitToNull(char * byte)
{
  byte ^= 0b00001000;
}

void Project::setSixthBitToNull(char * byte)
{
  byte ^= 0b00000100;
}

void Project::setSeventhBitToNull(char * byte)
{
  byte ^= 0b00000010;
}

void Project::setEigthBitToNull(char * byte)
{
  byte ^= 0b00000001;
}
*/
// INLJoin
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) : leftInput(leftIn), rightInput(rightIn), cond(condition) {
}

RC INLJoin::getNextTuple(void *data) {
  return -1;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
  TableScan * t_ptr = dynamic_cast<TableScan *>(leftInput);
  if (!t_ptr)
    return;
  cout<< "casted properly\n";
  attrs = t_ptr->attrs;
}
