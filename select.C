#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
    // Qu_Select sets up things and then calls ScanSelect to do the actual work
    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            std::cout << "failed 0" << std::endl;
            return status;
        }
    }

    AttrDesc attrDesc;
    Operator our_operator = op;
    if (attr != nullptr)
    {
        Status status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
        if (status != OK)
        {
            std::cout << "failed 0.5" << std::endl;
            return status;
        }
    }
    else
    {
		strcpy(attrDesc.relName, attrDescArray[0].relName);
        our_operator = EQ;
        attrValue = nullptr;
    }

    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }
    Status status = ScanSelect(result, projCnt, attrDescArray, &attrDesc, our_operator, attrValue, reclen);
    return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
	Status status;
    InsertFileScan resultRel(result, status);
    if (status != OK)
    {
        std::cout << "failed 1" << std::endl;
        return status;
    }

    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

	HeapFileScan scan(string(attrDesc->relName), status);
	if (status != OK) {
		cout << "failed 2" << endl;
		return status;
	}

	int i;
	float f;

	if (filter == nullptr) {
		status = scan.startScan(0, 0, STRING, NULL, EQ);
	}
	else {
		switch(attrDesc->attrType) {
			char* value;
			case INTEGER:
				value = (char*) filter;
				i = atoi(value);
				status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, (char *)&i, op);
			case FLOAT:
				value = (char*) filter;
				f = atof(value);
				status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, (char *)&f, op);
			default:
				status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, filter, op);
		}
	}	

    if (status != OK)
    {
        std::cout << "failed 3" << std::endl;
        return status;
    }

    RID rid;
    // iterate through scan and copy matching records into output
    while (scan.scanNext(rid) == OK)
    {
        Record record;
        status = scan.getRecord(record);
        if (status != OK)
        {
            std::cout << "failed 4" << std::endl;
            return status;
        }

        // we have a match, copy data into the output record
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++)
        {
            memcpy(outputData + outputOffset, (char*)record.data + projNames[i].attrOffset, projNames[i].attrLen);

            outputOffset += projNames[i].attrLen;
        } // end copy attrs

        // add the new record to the output relation
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        if (status != OK)
        {
            std::cout << "failed 5" << std::endl;
            return status;
        }
    }
    return OK;
}
