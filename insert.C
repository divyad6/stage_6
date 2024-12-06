#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	Status status;

	// get attribute descriptor for the relation
	AttrDesc *relAttrList;
	int relAttrCnt;
	status = attrCat->getRelInfo(relation, relAttrCnt, relAttrList);
	if (status != OK) {
		return status;
	}

	// order of the attributes in attrList may not be the same in the relation,
	// so rearrange attributes before insertion
	attrInfo* attrListCopy = new attrInfo[attrCnt];
	copy(attrList, attrList + attrCnt, attrListCopy);

	for (int i = 0 ; i < attrCnt ; i++) {
		if (strcmp(attrListCopy[i].attrName, relAttrList[i].attrName) != 0) {
			attrInfo temp;

			// search for the attribute
			int attrLocation = -1;

			for (int j = 0; j < attrCnt; j++) {
				if (strcmp(attrListCopy[j].attrName, relAttrList[i].attrName) == 0) {
					attrLocation = j;
					break;
				}
			}

			if (attrLocation == -1) {
				return ATTRNOTFOUND;
			}

			// swap attributes
			temp = attrListCopy[i];
			attrListCopy[i] = attrListCopy[attrLocation];
			attrListCopy[attrLocation] = temp;
		}
	}

	// check that attribute types match
	for (int i = 0 ; i < attrCnt ; i++) {
		if (attrListCopy[i].attrType != relAttrList[i].attrType) {
			return ATTRTYPEMISMATCH;
		}
	}

	// copy attribute values into data buffer
	int recLen = 0;
	for (int i = 0 ; i < attrCnt ; i++) {
		recLen += relAttrList[i].attrLen;
	}
	char *data = new char[recLen];

	int offset = 0;
	int val = 0;
	int floatval = 0;
	for (int i = 0 ; i < attrCnt ; i++) {
		switch(attrListCopy[i].attrType) {
			case STRING:
				memcpy(data + offset, attrListCopy[i].attrValue, relAttrList[i].attrLen);
				break;
			case INTEGER:
				val = atoi((char *)attrListCopy[i].attrValue);
				memcpy(data + offset, &val, relAttrList[i].attrLen);
				break;
			case FLOAT:
				floatval = atof((char *)attrListCopy[i].attrValue);
				memcpy(data + offset, &floatval, relAttrList[i].attrLen);
				break;
		}

		offset += relAttrList[i].attrLen;
	}

	// insert record into relation
	Record rec;
	rec.data = data;
	rec.length = recLen;
	InsertFileScan insertFileScan(relation, status);
	if (status != OK) {
		return status;
	}

	RID rid;
	status = insertFileScan.insertRecord(rec, rid);
	if (status != OK) {
		return status;
	}

	return OK;
}