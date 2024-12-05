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
    cout << "Doing QU_Select " << endl;

    AttrDesc attrDescArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        std::cout << "Getting attrDescArray " << i << std::endl;
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         attrDescArray[i]);
        if (status != OK)
        {
            std::cout << "failed 0" << std::endl;
            return status;
        }
    }

    std::cout << "Getting attrDesc" << std::endl;

    AttrDesc attrDesc;
    Status status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
    if (status != OK)
    {
        std::cout << "failed 0.5" << std::endl;
        return status;
    }

    std::cout << "Getting reclen" << std::endl;

    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }

    std::cout << "Calling ScanSelect" << std::endl;

    ScanSelect(result, projCnt, attrDescArray, &attrDesc, op, attrValue, reclen);

    std::cout << "Called ScanSelect" << std::endl;
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
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

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

    HeapFileScan scan(attrDesc->relName, status);
    if (status != OK)
    {
        std::cout << "failed 2" << std::endl;
        return status;
    }

    status = scan.startScan(attrDesc->attrOffset,
                                 attrDesc->attrLen,
                                 (Datatype) attrDesc->attrType,
                                 filter,
                                 op);
    if (status != OK)
    {
        std::cout << "failed 3" << std::endl;
        return status;
    }

    RID rid;
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
