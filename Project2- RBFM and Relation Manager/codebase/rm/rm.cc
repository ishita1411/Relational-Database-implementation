#include "rm.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <iostream>


RelationManager *RelationManager::_rm = 0;

RelationManager *RelationManager::instance() {
    if (!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager() {
    rbfm_obj = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager() {
}

RC RelationManager::rmgetByteForNullsIndicator(int fieldCount) {

    return ceil((double) fieldCount / CHAR_BIT);
}


bool RelationManager::alreadyExists(const string &fileName) {
    ifstream file(fileName);
    if (file.good()) {
        file.close();
        return true;
    }
    return false;
}

void RelationManager::prepareRecordColumns(int fieldCount,
                                           unsigned char *nullFieldsIndicator, int id, const int columnNameLength,
                                           const string &columnName, const int columnType, const int columnLength,
                                           const int columnPosition, void *buffer, int *recordSizeColumn) {
    bool nullBit = false;
    int offset = 0;
    // Null-indicators

    int nullFieldsIndicatorActualSize = rmgetByteForNullsIndicator(fieldCount);

    // Null-indicator for the fields
    memcpy((char *) buffer + offset, nullFieldsIndicator,
           nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    //(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
    nullBit = nullFieldsIndicator[0] & (1 << 7);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &id, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char *) buffer + offset, &columnNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, columnName.c_str(), columnNameLength);
        offset += columnNameLength;
    }

    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char *) buffer + offset, &columnType, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullFieldsIndicator[0] & (1 << 4);
    if (!nullBit) {
        memcpy((char *) buffer + offset, &columnLength, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullFieldsIndicator[0] & (1 << 3);
    if (!nullBit) {
        memcpy((char *) buffer + offset, &columnPosition, sizeof(int));
        offset += sizeof(int);
    }

    *recordSizeColumn = offset;
}

void RelationManager::prepareRecordTable(int fieldCount,
                                         unsigned char *nullFieldsIndicator, const int tableNameLength,
                                         const int fileNameLength, const string &tableName,
                                         const string &fileName, const int id, void *buffer,
                                         int *recordSizeTable) {

    int offset = 0;

    // Null-indicators
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = rmgetByteForNullsIndicator(fieldCount);

    memcpy((char *) buffer + offset, nullFieldsIndicator,
           nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    nullBit = nullFieldsIndicator[0] & (1 << 7);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &id, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char *) buffer + offset, &tableNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, tableName.c_str(), tableNameLength);
        offset += tableNameLength;
    }

    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char *) buffer + offset, &fileNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, fileName.c_str(), fileNameLength);
        offset += fileNameLength;
    }

    *recordSizeTable = offset;
}

void RelationManager::createRecordDescriptorTable(
        vector<Attribute> &recordDescriptor) {

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

}

void RelationManager::createRecordDescriptorColumns(
        vector<Attribute> &recordDescriptor) {
//(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

}

void RelationManager::insertCatalog(const vector<Attribute> recordDescriptor,
                                    const int tableNameLength,
                                    const int fileNameLength, const string &tableName, const string &fileName,
                                    const int idTable,
                                    int *recordSizeTable, int *recordSizeColumn, RID &ridTable, RID &ridColumn)
{
    void *recordTable = malloc(100);
    vector<Attribute> recordDescriptorTable;
    vector<Attribute> recordDescriptorColumn;
    createRecordDescriptorTable(recordDescriptorTable);
    createRecordDescriptorColumns(recordDescriptorColumn);
    FileHandle fileHandleTables;
    FileHandle fileHandleColumns;

    rbfm_obj->openFile(fileNameTable, fileHandleTables);
    rbfm_obj->openFile(fileNameColumn, fileHandleColumns);

    int fieldCountTable = (int) recordDescriptorTable.size();
    int fieldCountColumn = (int) recordDescriptorColumn.size();
    int nullFieldsIndicatorActualSizeT = rmgetByteForNullsIndicator(fieldCountTable);
    int nullFieldsIndicatorActualSizeC = rmgetByteForNullsIndicator(fieldCountColumn);

    unsigned char *nullsIndicatorTable = (unsigned char *) malloc(
            nullFieldsIndicatorActualSizeT);
    memset(nullsIndicatorTable, 0, nullFieldsIndicatorActualSizeT);
    unsigned char *nullsIndicatorColumn = (unsigned char *) malloc(
            nullFieldsIndicatorActualSizeC);
    memset(nullsIndicatorColumn, 0, nullFieldsIndicatorActualSizeC);


    prepareRecordTable(recordDescriptorTable.size(), nullsIndicatorTable, tableNameLength, fileNameLength,
                       tableName, fileName, idTable, recordTable, recordSizeTable);
    rbfm_obj->insertRecord(fileHandleTables, recordDescriptorTable, recordTable, ridTable);

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        void *recordColumn = malloc(100);
        string columnName = recordDescriptor[i].name;
        const int columnType = recordDescriptor[i].type;
        const int columnLength = recordDescriptor[i].length;
        prepareRecordColumns(recordDescriptorColumn.size(), nullsIndicatorColumn,
                             idTable, columnName.length(), columnName, columnType, columnLength, (i + 1), recordColumn,
                             recordSizeColumn);
        rbfm_obj->insertRecord(fileHandleColumns, recordDescriptorColumn, recordColumn, ridColumn);
        free(recordColumn);
    }
    free(recordTable);
    rbfm_obj->closeFile(fileHandleTables);
    rbfm_obj->closeFile(fileHandleColumns);
}

void RelationManager::updateTableID(int tableID) {
    ofstream tableIDfile;
    tableIDfile.open("tableIDfile.txt");
    tableIDfile << tableID;
    tableIDfile.close();
}

RC RelationManager::getTableID() {
    int tableID;
    ifstream infile;
    infile.open("tableIDfile.txt");

    infile >> tableID;
    return tableID;
}

RC RelationManager::createCatalog()
{

    if (alreadyExists(fileNameTable) or alreadyExists(fileNameColumn)) {
        return -1;
    }
    string tableName = "Tables";
    RID ridTable;
    RID ridColumn;

    int recordSizeTable = 0;
    int recordSizeColumn = 0;
    int idTable = 1;

    vector<Attribute> recordDescriptorTable;
    vector<Attribute> recordDescriptorColumn;
    createRecordDescriptorTable(recordDescriptorTable);
    createRecordDescriptorColumns(recordDescriptorColumn);



    rbfm_obj->createFile(fileNameTable);
    rbfm_obj->createFile(fileNameColumn);

    insertCatalog(recordDescriptorTable,
                  tableName.length(), fileNameTable.length(),
                  tableName, fileNameTable, idTable, &recordSizeTable, &recordSizeColumn, ridTable, ridColumn);
    idTable++;
    string tableName2 = "Columns";
    insertCatalog(recordDescriptorColumn,
                  tableName2.length(), fileNameColumn.length(),
                  tableName2, fileNameColumn, idTable, &recordSizeTable, &recordSizeColumn, ridTable, ridColumn);
    updateTableID(idTable);
    return 0;
}

RC RelationManager::deleteCatalog() {
    rbfm_obj->destroyFile(fileNameTable);
    rbfm_obj->destroyFile(fileNameColumn);
    return 0;
}

RC RelationManager::createTable(const string &tableName,
                                const vector<Attribute> &attrs) {

    if (alreadyExists(tableName)) {
        return -1;
    }

    int idTable = getTableID() + 1;

    RID ridTable;
    RID ridColumn;
    string fileName = tableName;
    int recordSizeT = 0;
    int recordSizeC = 0;
    FileHandle tableFileHandle;
    rbfm_obj->createFile(tableName);
    insertCatalog(attrs, tableName.length(), fileName.length(),
                  tableName, fileName, idTable, &recordSizeT, &recordSizeC, ridTable, ridColumn);
    updateTableID(idTable);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName) {
    if(tableName.compare(fileNameTable) == 0 || tableName.compare(fileNameColumn) == 0) return -1;
    vector<string> attrTableID = {"table-id"};
    vector<string> attributes = {"column-name"};
    vector<Attribute> recDesTable;
    createRecordDescriptorTable(recDesTable);
    FileHandle fileHandleTable;
    rbfm_obj->openFile(tableName, fileHandleTable);
    void *dataT = malloc(100);
    void *dataC = malloc(100);
    RID ridT;
    RID ridC;
    void *tableID = malloc(10);
    int ID;
    RM_ScanIterator rmsiT;
    RM_ScanIterator rmsiC;

    int vlen= tableName.length();
    void* value = malloc(vlen + sizeof(int));
    memcpy(value, &vlen, sizeof(int));
    memcpy((char *)value + sizeof(int), tableName.c_str(), vlen);

    scan(fileNameTable, "table-name", EQ_OP, value, attrTableID, rmsiT);
    while (rmsiT.getNextTuple(ridT, dataT) != RM_EOF) {
        readAttribute(tableName, ridT, "table-id", tableID);
        memcpy(&ID, (char *) tableID + sizeof(char), sizeof(int));
    }
    deleteTuple(tableName, ridT);


    scan(fileNameColumn, "table-id", EQ_OP, &ID, attributes, rmsiC);
    while (rmsiC.getNextTuple(ridC, dataC) != RM_EOF) {
        deleteTuple(tableName, ridC);
    }
    rbfm_obj->destroyFile(tableName);
    free(dataC);
    free(dataT);
    free(tableID);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    void *record_data = malloc(1000);
    void *column = malloc(1000);
    void *table_ID = malloc(10);
    int ID;
    RID rid;

    int varcharLength;

    int vlen= tableName.length();

    RBFM_ScanIterator rmsiTable;
    RBFM_ScanIterator rmsiColumn;

    FileHandle FHColumns;
    FileHandle FHTables;
    rbfm_obj->openFile(fileNameTable, FHTables);
    rbfm_obj->openFile(fileNameColumn, FHColumns);

    vector<Attribute> recordDesTable;
    vector<Attribute> recordDesColumn;
    createRecordDescriptorTable(recordDesTable);
    createRecordDescriptorColumns(recordDesColumn);
    unsigned char *nullsIndicator = (unsigned char *) malloc(1);
    memset(nullsIndicator, 0, 1);

    vector<string> attrTableID = {"table-id"};

    vector<string> attributes = {"column-name", "column-type", "column-length"};


    void* value = malloc(vlen + sizeof(int));
    memcpy(value, &vlen, sizeof(int));
    memcpy((char *)value + sizeof(int), tableName.c_str(), vlen);


    rbfm_obj->scan(FHTables, recordDesTable, "table-name", EQ_OP, value, attrTableID, rmsiTable);
    while (rmsiTable.getNextRecord(rid, record_data) != RBFM_EOF) {
        rbfm_obj->readAttribute(FHTables, recordDesTable, rid, "table-id", table_ID);
        memcpy(&ID, (char *) table_ID + sizeof(char), sizeof(int));
    }
    rbfm_obj->scan(FHColumns, recordDesColumn, "table-id", EQ_OP, &ID, attributes, rmsiColumn);
    int k = 0;
    Attribute attribute;
    //table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int
    while (rmsiColumn.getNextRecord(rid, record_data) != RBFM_EOF) {
        //Column Name
        AttrType type;
        AttrLength length;
        rbfm_obj->readAttribute(FHColumns, recordDesColumn, rid, attributes[0], column);
        memcpy(&varcharLength, (char *) column + 1, 4);
        char name[varcharLength + 1];
        memcpy(&name, (char *) column + 1 + 4, varcharLength);
        name[varcharLength] = '\0';

        rbfm_obj->readAttribute(FHColumns, recordDesColumn, rid, attributes[1], column);
        memcpy(&type, (char *) column + 1, 4);

        rbfm_obj->readAttribute(FHColumns, recordDesColumn, rid, attributes[2], column);
        memcpy(&length, (char *) column + 1, 4);

        attribute.type = type;
        attribute.name = name;
        attribute.length = length;
        attrs.push_back(attribute);

        k++;
    }
    rbfm_obj->closeFile(FHColumns);
    rbfm_obj->closeFile(FHTables);
    free(column);
    free(record_data);

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
                                RID &rid) {
    vector<Attribute> recDes;
    getAttributes(tableName, recDes);
    FileHandle fileHandleT;
    RC rc;
    RC response = rbfm_obj->openFile(tableName, fileHandleT);

    if (response == 0)
    {
        rc = rbfm_obj->insertRecord(fileHandleT, recDes, data, rid);
        rbfm_obj->closeFile(fileHandleT);
    } else {
        rc = -1;
    }
    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    vector<Attribute> recDes;
    getAttributes(tableName, recDes);
    FileHandle fileHandleT;
    RC code;
    RC response = rbfm_obj->openFile(tableName, fileHandleT);
    if (response == 0)
    {
        code = rbfm_obj->deleteRecord(fileHandleT, recDes, rid);
        rbfm_obj->closeFile(fileHandleT);
    } else {
        code = -1;
    }
    return code;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
                                const RID &rid) {
    vector<Attribute> recDes;
    getAttributes(tableName, recDes);
    FileHandle fileHandleT;
    RC code;
    RC response=rbfm_obj->openFile(tableName, fileHandleT);
    if (response == 0) {
        code = rbfm_obj->updateRecord(fileHandleT, recDes, data, rid);
        rbfm_obj->closeFile(fileHandleT);
    } else {
        code = -1;
    }
    return code;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
                              void *data) {
    vector<Attribute> recDes;
    getAttributes(tableName, recDes);
    FileHandle fileHandleT;
    RC code;
    RC response = rbfm_obj->openFile(tableName, fileHandleT);
    if (response == 0) {
        code = rbfm_obj->readRecord(fileHandleT, recDes, rid, data);
        rbfm_obj->closeFile(fileHandleT);
    } else {
        code = -1;
    }
    return code;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs,
                               const void *data) {
    rbfm_obj->printRecord(attrs, data);
    return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
                                  const string &attributeName, void *data) {
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    FileHandle fileTable;
    RC rc;
    if (rbfm_obj->openFile(tableName, fileTable) == 0) {
        rc = rbfm_obj->readAttribute(fileTable, recordDescriptor, rid, attributeName, data);
        rbfm_obj->closeFile(fileTable);
    } else {
        rc = -1;
    }
    return rc;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute, const CompOp compOp,
                         const void *value, const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    RBFM_ScanIterator rbfmsi;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    FileHandle fileTable;
    RC rc;
    if (rbfm_obj->openFile(tableName, fileTable) == 0) {
        rc = rbfm_obj->scan(fileTable, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfmsi);
        rm_ScanIterator.initialize(rbfmsi);
    } else {
        rc = -1;
    }
    return rc;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName,
                                  const string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName,
                                 const Attribute &attr) {
    return -1;
}


/***********************************************************************************************************************
 * RM Scan Iterator
 **********************************************************************************************************************/

void RM_ScanIterator::initialize(RBFM_ScanIterator scanIterator) {
    _scanIterator = scanIterator;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    RC rc = _scanIterator.getNextRecord(rid, data);
    return (rc == RBFM_EOF) ? RM_EOF : rc;
}

RC RM_ScanIterator::close() {
    return _scanIterator.close();
}
