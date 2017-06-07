
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
	cout<< "casted properly\n";
//	while (t-ptr->iter->getNextTuple(data) != QE_EOF){
//
//	}
  return -1;
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
