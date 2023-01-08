#include <cstring>
#include <fstream>
#include <cmath>
#include <algorithm>
#include <iomanip>
#include "ManageSystem.h"

using namespace std;

int SystemManager::getTableIDByName(const string &tableName) {
    int table_id = -1;
    for (int i = 0; i < _tableNum; i++) {
        if (_tables[i]._tableName == tableName) {
            table_id = i;
            break;
        }
    }
    return table_id;
}

int SystemManager::getFileIDByName(const std::string &tableName) {
    return _tableName2fileID[tableName];
}

int SystemManager::getAttrIDByName(const TableInfo &tableInfo, const string &attrName) {
    int attr_id = -1;
    for (int i = 0; i < tableInfo._attrNum; i++) {
        if (tableInfo._attrs[i]._attrName == attrName) {
            attr_id = i;
            break;
        }
    }
    return attr_id;
}

bool SystemManager::checkForeignConstraint(const TableInfo &tableInfo, const TableInfo &refTableInfo, const std::vector<std::string> &foreignKey) {
    //外键和主键长度是否一致
    if (refTableInfo._primaryKeys.size() != foreignKey.size()) return false;
    //外键和主键字段类型是否一一对应
    for (int i = 0; i < refTableInfo._primaryKeys.size(); i++) {
        const AttrInfo &attrInfo = tableInfo._attrs[getAttrIDByName(tableInfo, foreignKey[i])];
        const AttrInfo &refAttrInfo = refTableInfo._attrs[getAttrIDByName(refTableInfo, refTableInfo._primaryKeys[i])];
        if (refAttrInfo._attrType != attrInfo._attrType) return false;
        if (refAttrInfo._attrLength != attrInfo._attrLength) return false;
    }
    return true;
}

const TableInfo &SystemManager::getTableInfoByID(int id) {
    return _tables[id];
}

SystemManager::SystemManager(BufPageManager *bufPageManager, IndexManager *indexManager, RecordManager *recordManager) {
    _bufPageManager = bufPageManager;
    _indexManager = indexManager;
    _recordManager = recordManager;
    _tableNum = 0;
    system("ls > temp.log");
    ifstream fin("temp.log");
    string dbName;
    while (fin >> dbName) {
        auto len = dbName.length();
        if (len > 3 && dbName.substr(len - 3, 3) == ".db") {
            _dbNames.push_back(dbName.substr(0, len - 3));
        }
    }
    system("rm temp.log");
    fin.close();
}

string SystemManager::getDBName() {
    return _dbName;
}

int SystemManager::getTableNum() {
    return _tableNum;
}

bool SystemManager::createDB(const std::string &dbName) {
    //检查数据库是否存在
    if (find(_dbNames.begin(), _dbNames.end(), dbName) != _dbNames.end()) {
        cerr << "Database " + dbName + " already exists!" << endl;
        return false;
    }
    system(("mkdir " + dbName + ".db").c_str());//创建文件夹
    chdir((dbName + ".db").c_str());
    ofstream fout("meta.db");//创建元信息文件
    if (fout.fail()) {
        cerr << "Open meta.db failed!" << endl;
        return false;
    }
    fout << 0 << endl;//表数量为0
    fout.close();
    _dbNames.push_back(dbName);
    chdir("..");//切换目录
    return true;
}

bool SystemManager::dropDB(const std::string &dbName) {
    //检查数据库是否存在
    int pos = -1;
    for (int i = 0; i < _dbNames.size(); i++) {
        if (_dbNames[i] == dbName) {
            pos = i;
            break;
        }
    }
    if (pos == -1) {
        cerr << "Database " + dbName + " does not exists!" << endl;
        return false;
    } else {
        _dbNames.erase(_dbNames.begin() + pos);
        system(("rm -r " + dbName + ".db").c_str());//删除文件夹
    }
    return true;
}

bool SystemManager::openDB(const string &dbName) {
    //检查数据库是否存在
    if (find(_dbNames.begin(), _dbNames.end(), dbName) == _dbNames.end()) {
        cerr << "Database " + dbName + " does not exist!" << endl;
        return false;
    }
    _dbName = dbName;
    chdir((dbName + ".db").c_str());//切换目录
    ifstream fin("meta.db");//打开元信息文件
    if (fin.fail()) {
        cerr << "Open meta.db failed!" << endl;
        return false;
    }
    fin >> _tableNum;
    //读入表信息
    for (int i = 0; i < _tableNum; i++) {
        TableInfo tableInfo;
        fin >> tableInfo._tableName;
        fin >> tableInfo._attrNum;
        fin >> tableInfo._recordSize;
        fin >> tableInfo._indexNum;
        fin >> tableInfo._foreignKeyNum;
        fin >> tableInfo._uniqueNum;
        int offset = ceil(tableInfo._attrNum / 8.0);//NULL位图偏移
        //读入列信息
        for (int j = 0; j < tableInfo._attrNum; j++) {
            AttrInfo attrInfo;
            fin >> attrInfo._attrName;
            string type;
            fin >> type;
            if (type == "INT") {
                attrInfo._attrType = INTEGER;
                attrInfo._attrLength = 4;
            } else if (type == "FLOAT") {
                attrInfo._attrType = FLOAT;
                attrInfo._attrLength = 4;
            } else {
                attrInfo._attrType = STRING;
                fin >> attrInfo._attrLength;
            }
            attrInfo._offset = offset;
            offset += attrInfo._attrLength;
            fin >> attrInfo._notNull >> attrInfo._hasDefault >> attrInfo._isPrimary;
            //是否为主键
            if (attrInfo._isPrimary) {
                tableInfo._primaryKeys.push_back(attrInfo._attrName);
            }
            //是否有默认值
            if (attrInfo._hasDefault) {
                string value;
                fin >> value;
                if (value != "NULL") {
                    if (type == "INT") {
                        attrInfo._defaultValue = new int;
                        *((int *) attrInfo._defaultValue) = stoi(value);
                    } else if (type == "FLOAT") {
                        attrInfo._defaultValue = new float;
                        *((float *) attrInfo._defaultValue) = stof(value);
                    } else if (type == "VARCHAR") {
                        attrInfo._defaultValue = new char[attrInfo._attrLength];
                        memset(attrInfo._defaultValue, 0, attrInfo._attrLength);
                        memcpy((char *) attrInfo._defaultValue, value.c_str(), value.length());
                    }
                } else attrInfo._defaultValue = nullptr;
            } else attrInfo._defaultValue = nullptr;
            tableInfo._attrs.push_back(attrInfo);
        }
        //读入索引信息
        for (int j = 0; j < tableInfo._indexNum; j++) {
            vector<string> index;
            int indexNum;//索引包含的列数量
            fin >> indexNum;
            for (int k = 0; k < indexNum; k++) {
                string attrName;
                fin >> attrName;
                index.push_back(attrName);
            }
            tableInfo._indexes.push_back(index);
        }
        //读入外键信息
        for (int j = 0; j < tableInfo._foreignKeyNum; j++) {
            string foreignKeyName, reference;//外键名称，关联表名
            fin >> foreignKeyName >> reference;
            tableInfo._foreignKeyNames.push_back(foreignKeyName);
            tableInfo._references.push_back(reference);
            vector<string> foreignKey;
            int foreignKeyNum;//外键包含的列数量
            fin >> foreignKeyNum;
            for (int k = 0; k < foreignKeyNum; k++) {
                string attrName;
                fin >> attrName;
                foreignKey.push_back(attrName);
            }
            tableInfo._foreignKeys.push_back(foreignKey);
        }
        //读入unique信息
        for (int j = 0; j < tableInfo._uniqueNum; j++) {
            vector<string> unique;
            int uniqueNum;//unique包含的列数量
            fin >> uniqueNum;
            for (int k = 0; k < uniqueNum; k++) {
                string attrName;
                fin >> attrName;
                unique.push_back(attrName);
            }
            tableInfo._uniques.push_back(unique);
        }
        _tables.push_back(tableInfo);
        //打开文件
        int fileID;
        if (!_recordManager->openFile(tableInfo._tableName.c_str(), fileID)) {
            cerr << "Open file " + tableInfo._tableName + " failed!" << endl;
            return false;
        }
        _tableName2fileID[tableInfo._tableName] = fileID;//建立表名到文件描述符的映射
    }
    fin.close();
    return true;
}

bool SystemManager::closeDB() {
    _dbName.clear();
    ofstream fout("meta.db");//打开元信息文件
    if (fout.fail()) {
        cerr << "Open meta.db failed!" << endl;
        return false;
    }
    fout << _tableNum << endl << endl;
    //输出表信息
    for (auto &tableInfo : _tables) {
        fout << tableInfo._tableName << " " << tableInfo._attrNum << " " << tableInfo._recordSize << " " << tableInfo._indexNum <<  " " << tableInfo._foreignKeyNum << " " << tableInfo._uniqueNum << endl;
        //输出列信息
        for (auto &attrInfo : tableInfo._attrs) {
            fout << attrInfo._attrName << " ";
            if (attrInfo._attrType == INTEGER) {
                fout << "INT" << " ";
            } else if (attrInfo._attrType == FLOAT) {
                fout << "FLOAT" << " ";
            } else {
                fout << "VARCHAR" << " " << attrInfo._attrLength << " ";
            }
            fout << attrInfo._notNull << " " << attrInfo._hasDefault << " " << attrInfo._isPrimary;
            if (attrInfo._hasDefault) {
                if (attrInfo._defaultValue != nullptr) {
                    if (attrInfo._attrType == INTEGER) {
                        fout << " " << *((int *) attrInfo._defaultValue) << endl;
                        delete (int *) attrInfo._defaultValue;
                    } else if (attrInfo._attrType == FLOAT) {
                        fout << " " << *((float *) attrInfo._defaultValue) << endl;
                        delete (float *) attrInfo._defaultValue;
                    } else {
                        fout << " " << ((char *) attrInfo._defaultValue) << endl;
                        delete[](char *) attrInfo._defaultValue;
                    }
                } else fout << " NULL" << endl;
                attrInfo._defaultValue = nullptr;
            } else fout << endl;
        }
        //输出索引信息
        for (int j = 0; j < tableInfo._indexNum; j++) {
            int indexNum = (int)tableInfo._indexes[j].size();
            fout << indexNum;//索引包含的列数量
            for (int k = 0; k < indexNum; k++) {
                fout << " " << tableInfo._indexes[j][k];
            }
            fout << endl;
            tableInfo._indexes[j].clear();
        }
        //输出外键信息
        for (int j = 0; j < tableInfo._foreignKeyNum; j++) {
            fout << tableInfo._foreignKeyNames[j] <<  " " << tableInfo._references[j] << " ";
            vector<string> foreignKey;
            int foreignKeyNum = (int)tableInfo._foreignKeys[j].size();
            fout << foreignKeyNum;//外键包含的列数量
            for (int k = 0; k < foreignKeyNum; k++) {
                fout << " " << tableInfo._foreignKeys[j][k];
            }
            fout << endl;
            tableInfo._foreignKeys[j].clear();
        }
        //输出unique信息
        for (int j = 0; j < tableInfo._uniqueNum; j++) {
            int uniqueNum = (int)tableInfo._uniques[j].size();
            fout << uniqueNum;//unique包含的列数量
            for (int k = 0; k < uniqueNum; k++) {
                fout << " " << tableInfo._uniques[j][k];
            }
            fout << endl;
            tableInfo._uniques[j].clear();
        }
        //清除记录
        tableInfo._indexes.clear();
        tableInfo._uniques.clear();
        tableInfo._primaryKeys.clear();
        tableInfo._foreignKeyNames.clear();
        tableInfo._foreignKeys.clear();
        tableInfo._references.clear();
        tableInfo._attrs.clear();
        //关闭文件
        if (!_recordManager->closeFile(_tableName2fileID[tableInfo._tableName])) {
            cerr << "Close file " + tableInfo._tableName + " failed!" << endl;
            return false;
        }
        fout << endl;
    }
    _tables.clear();//清除表
    _tableName2fileID.clear();//清除表名到文件描述符的映射
    fout.close();
    chdir("..");//切换目录
    return true;
}

bool SystemManager::createTable(const TableInfo &tableInfo) {
    //检查表是否存在
    for (int i = 0; i < _tableNum; i++) {
        if (_tables[i]._tableName == tableInfo._tableName) {
            cerr << "Table " + tableInfo._tableName + " already exists!" << endl;
            return false;
        }
    }
    //外键不能指向自己
    for (const auto &reference : tableInfo._references) {
        if (reference == tableInfo._tableName) {
            cerr << "Reference cannot be the same table!" << endl;
            return false;
        }
    }
    //检查外键约束
    for (int i = 0; i < tableInfo._foreignKeyNum; i++) {
        for (const auto &table : _tables) {
            if (table._tableName == tableInfo._references[i]) {
                bool constraint = checkForeignConstraint( tableInfo, table, tableInfo._foreignKeys[i]);
                if (!constraint) {
                    cerr << "Invalid foreign key!" << endl;
                    return false;
                }
                break;
            }
        }
    }
    //创建表文件
    _recordManager->createFile(tableInfo._tableName.c_str(), tableInfo._recordSize);
    int fileID;
    if (!_recordManager->openFile(tableInfo._tableName.c_str(), fileID)) {
        cerr << "Open file " + tableInfo._tableName + " failed!" << endl;
        return false;
    }
    _tables.push_back(tableInfo);
    _tableName2fileID[tableInfo._tableName] = fileID;
    _tableNum++;
    return true;
}

bool SystemManager::dropTable(const string &tableName) {
    //检查表是否存在
    int id = getTableIDByName(tableName);
    if (id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    //检查是否是其它表的参照
    for (int i = 0; i < _tableNum; i++) {
        if (find(_tables[i]._references.begin(), _tables[i]._references.end(), tableName) != _tables[i]._references.end()) {
            cerr << "Foreign key on table " << tableName << "!" << endl;
            return false;
        }
    }
    //关闭表文件
    if (!_recordManager->closeFile(_tableName2fileID[tableName])) {
        cerr << "Close file " + tableName + " failed!" << endl;
        return false;
    }
    //删除表文件
    if (!_recordManager->destroyFile(tableName.c_str())) {
        cerr << "Destroy file " + tableName + " failed!" << endl;
        return false;
    }
    //删除可能存在的索引、主键等
    if (!_tables[id]._primaryKeys.empty() || !_tables[id]._foreignKeys.empty() || !_tables[id]._uniques.empty() || !_tables[id]._indexes.empty()) {
        system(("rm " + tableName + ".*").c_str());
    }
    _tables.erase(_tables.begin() + id);
    _tableName2fileID.erase(tableName);
    _tableNum--;
    return true;
}

bool SystemManager::createIndex(const string &tableName, const vector<string> &attrNames) {
    //检查表是否存在
    int table_id = getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    //检查索引是否存在
    TableInfo &tableInfo = _tables[table_id];
    for (const auto &index : tableInfo._indexes) {
        if (index == attrNames) {
            cerr << "Can not create duplicated indexes!" << endl;
            return false;
        }
    }
    //检查对应列是否存在、非空
    for (auto &attrName : attrNames) {
        int attr_id = getAttrIDByName(tableInfo, attrName);
        if (attr_id == -1) {
            cerr << "Column " << attrName << " does not exist!" << endl;
            return false;
        }
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        if (!attrInfo._notNull) {
            cerr << "Column " << attrName << " must be not null!" << endl;
            return false;
        }
    }
    //以下根据记录建立索引
    int attrNum = (int) attrNames.size();
    auto *attrTypes = new AttrType[attrNum];
    int *attrLens = new int[attrNum];
    int attrLen = 0;//索引总大小，单位：字节
    for (int i = 0; i < attrNum; i++) {
        int attr_id = getAttrIDByName(tableInfo, attrNames[i]);
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        attrTypes[i] = attrInfo._attrType;
        attrLens[i] = attrInfo._attrLength;
        attrLen += attrInfo._attrLength;
    }
    //创建索引文件
    _indexManager->createIndex(tableName.c_str(), attrNames, attrNum, attrLens, attrTypes);
    int fileID;
    _indexManager->openIndex(tableName.c_str(), attrNames, fileID);
    IndexHandle indexHandle(_bufPageManager, fileID);
    RecordHandle recordHandle(_bufPageManager, _tableName2fileID[tableName]);
    //扫描每一条记录，插入一条索引
    if (recordHandle.openScan()) {
        RID rid;
        auto data = new char[tableInfo._recordSize];//记录数据
        auto index = new char[attrLen];//索引数据
        while (recordHandle.getNextRecord(rid, (BufType)data)) {
            int offset = 0;//索引数据偏移
            for (auto &attrName : attrNames) {
                int attr_id = getAttrIDByName(tableInfo, attrName);
                AttrInfo &attrInfo = tableInfo._attrs[attr_id];
                //从记录的对应位置拷贝到索引的对应位置
                memcpy(index + offset, data + attrInfo._offset, attrInfo._attrLength);
                offset += attrInfo._attrLength;
            }
            indexHandle.insertEntry((BufType)index, rid, false, false);
            memset(data, 0, tableInfo._recordSize);
            memset(index, 0, attrLen);
        }
        delete[] data;
        delete[] index;
    }
    //关闭索引文件
    _indexManager->closeIndex(fileID);
    tableInfo._indexNum++;
    tableInfo._indexes.push_back(attrNames);
    delete[] attrTypes;
    delete[] attrLens;
    return true;
}

bool SystemManager::dropIndex(const string &tableName, const vector<string> &attrNames) {
    //检查表是否存在
    int table_id = getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    TableInfo &tableInfo = _tables[table_id];
    //检查索引是否存在
    int pos = -1;
    for (int i = 0; i < tableInfo._indexNum; i++) {
        if (tableInfo._indexes[i] == attrNames) pos = i;
    }
    if (pos == -1) {
        cerr << "Index does not exist!" << endl;
        return false;
    }
    //删除索引文件
    _indexManager->destroyIndex(tableName.c_str(), attrNames);
    tableInfo._indexNum--;
    tableInfo._indexes.erase(tableInfo._indexes.begin() + pos);
    return true;
}

bool SystemManager::createPrimary(const string &tableName, const vector<string> &attrNames) {
    //检查表是否存在
    int table_id = getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    TableInfo &tableInfo = _tables[table_id];
    //检查是否已有主键
    if (!tableInfo._primaryKeys.empty()) {
        cerr << "Primary key already exists!" << endl;
        return false;
    }
    //主键不能是外键
    if (find(tableInfo._foreignKeys.begin(), tableInfo._foreignKeys.end(), attrNames) != tableInfo._foreignKeys.end()) {
        cerr << "Primary key can not be a foreign key!" << endl;
        return false;
    }
    //检查列是否存在、非空
    for (auto &attrName : attrNames) {
        int attr_id = getAttrIDByName(tableInfo, attrName);
        if (attr_id == -1) {
            cerr << "Column " << attrName << " does not exist!" << endl;
            return false;
        }
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        if (!attrInfo._notNull) {
            cerr << "Column " << attrName << " must be not null!" << endl;
            return false;
        }
    }
    //以下根据记录建立主键
    int attrNum = (int) attrNames.size();
    auto *attrTypes = new AttrType[attrNum];
    int *attrLens = new int[attrNum];
    int primaryKeySize = 0;//主键总大小，单位：字节
    for (int i = 0; i < attrNum; i++) {
        int attr_id = getAttrIDByName(tableInfo, attrNames[i]);
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        attrTypes[i] = attrInfo._attrType;
        attrLens[i] = attrInfo._attrLength;
        primaryKeySize += attrInfo._attrLength;
    }
    //创建主键文件，同索引文件，但要求不能重复
    _indexManager->createIndex(tableName.c_str(), vector<string>(1, "primary"), attrNum, attrLens, attrTypes);
    int fileID;
    _indexManager->openIndex(tableName.c_str(), vector<string>(1, "primary"), fileID);
    IndexHandle indexHandle(_bufPageManager, fileID);
    RecordHandle recordHandle(_bufPageManager, _tableName2fileID[tableName]);
    //扫描每一条记录，插入一条主键
    if (recordHandle.openScan()) {
        RID rid;
        auto data = new char[tableInfo._recordSize];
        auto primary = new char[primaryKeySize];
        while (recordHandle.getNextRecord(rid, (BufType)data)) {
            int offset = 0;//索引数据偏移
            for (auto &attrName : attrNames) {
                int attr_id = getAttrIDByName(tableInfo, attrName);
                AttrInfo &attrInfo = tableInfo._attrs[attr_id];
                //从记录的对应位置拷贝到主键的对应位置
                memcpy(primary + offset, data + attrInfo._offset, attrInfo._attrLength);
                offset += attrInfo._attrLength;
            }
            //检查是否重复，若重复则创建主键失败
            if (!indexHandle.insertEntry((BufType)primary, rid, true, false)) {
                _indexManager->closeIndex(fileID);
                _indexManager->destroyIndex(tableName.c_str(), vector<string>(1, "primary"));
                cerr << "Repetitive primary keys!" << endl;
                delete[] data;
                delete[] primary;
                delete[] attrTypes;
                delete[] attrLens;
                return false;
            }
            memset(data, 0, tableInfo._recordSize);
            memset(primary, 0, primaryKeySize);
        }
        delete[] data;
        delete[] primary;
    }
    //关闭主键文件
    _indexManager->closeIndex(fileID);
    for (auto &attrName : attrNames) {
        int attr_id = getAttrIDByName(tableInfo, attrName);
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        tableInfo._primaryKeys.push_back(attrInfo._attrName);
        attrInfo._isPrimary = true;
    }
    delete[] attrTypes;
    delete[] attrLens;
    return true;
}

bool SystemManager::dropPrimary(const string &tableName) {
    //检查表是否存在
    int table_id = getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    TableInfo &tableInfo = _tables[table_id];
    //检查主键是否存在
    if (tableInfo._primaryKeys.empty()) {
        cerr << "Primary key does not exist!" << endl;
        return false;
    }
    //检查是否有其它表参照该表
    for (int i = 0; i < _tableNum; i++) {
        if (find(_tables[i]._references.begin(), _tables[i]._references.end(), tableName) != _tables[i]._references.end()) {
            cerr << "Foreign key on table " << tableName << "!" << endl;
            return false;
        }
    }
    for (const auto &attrName: tableInfo._primaryKeys) {
        int attr_id = getAttrIDByName(tableInfo, attrName);
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        attrInfo._isPrimary = false;
    }
    //删除主键文件
    _indexManager->destroyIndex(tableName.c_str(), vector<string>(1, "primary"));
    tableInfo._primaryKeys.clear();
    return true;
}

bool SystemManager::createForeign(const std::string &tableName, const std::string &foreignKeyName, const std::vector<std::string> &attrNames, const std::string &reference, const vector<string> &referenceKeys) {
    //检查表是否存在
    int table_id = getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    //检查参照表是否存在
    int reference_table_id = getTableIDByName(reference);
    if (reference_table_id == -1) {
        cerr << "Reference table " << reference << " does not exist!" << endl;
        return false;
    }
    TableInfo &tableInfo = _tables[table_id];
    //检查外键是否重复
    for (const auto &foreignKey : tableInfo._foreignKeys) {
        if (foreignKey == attrNames) {
            cerr << "Can not create duplicated foreign key!" << endl;
            return false;
        }
    }
    //外键不能是主键
    if (attrNames == tableInfo._primaryKeys) {
        cerr << "Foreign key can not be a primary key!" << endl;
        return false;
    }
    //检查列是否存在、非空
    for (auto &attrName : attrNames) {
        int attr_id = getAttrIDByName(tableInfo, attrName);
        if (attr_id == -1) {
            cerr << "Column " << attrName << " does not exist!" << endl;
            return false;
        }
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        if (!attrInfo._notNull) {
            cerr << "Column " << attrName << " must be not null!" << endl;
            return false;
        }
    }
    TableInfo &refTableInfo = _tables[reference_table_id];
    //检查参照表是否有主键
    if (refTableInfo._primaryKeys.empty()) {
        cerr << "Reference table does not have primary key!" << endl;
        return false;
    }
    //外键只能关联到主键
    if (referenceKeys != refTableInfo._primaryKeys) {
        cerr << "Reference key must be primary key!" << endl;
        return false;
    }
    //检查外键约束
    if (!checkForeignConstraint(tableInfo, refTableInfo, attrNames)) {
        cerr << "Foreign key does not match primary key!" << endl;
        return false;
    }
    //以下检查外键所有值是否在参照表的主键中出现
    int attrNum = (int) attrNames.size();
    auto *attrTypes = new AttrType[attrNum];
    int *attrLens = new int[attrNum];
    int foreignKeySize = 0;//外键总大小，单位：字节
    for (int i = 0; i < attrNum; i++) {
        int attr_id = getAttrIDByName(tableInfo, attrNames[i]);
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        attrTypes[i] = attrInfo._attrType;
        attrLens[i] = attrInfo._attrLength;
        foreignKeySize += attrInfo._attrLength;
    }
    //创建foreign文件
    vector<string> foreignAttrNames = vector<string>(attrNames);
    foreignAttrNames.emplace_back("foreign");
    _indexManager->createIndex(tableName.c_str(), foreignAttrNames, attrNum, attrLens, attrTypes);
    int fileID1, fileID2;
    //打开参照表的主键文件
    _indexManager->openIndex(reference.c_str(), vector<string>(1, "primary"), fileID1);
    //打开本表的外键文件
    _indexManager->openIndex(tableName.c_str(), foreignAttrNames, fileID2);
    IndexHandle indexHandle1(_bufPageManager, fileID1);
    IndexHandle indexHandle2(_bufPageManager, fileID2);
    RecordHandle recordHandle(_bufPageManager, _tableName2fileID[tableName]);
    //扫描每一条记录，检查是否出现在参照表的主键文件里
    if (recordHandle.openScan()) {
        RID rid;
        auto data = new char[tableInfo._recordSize];
        auto foreign = new char[foreignKeySize];
        while (recordHandle.getNextRecord(rid, (BufType)data)) {
            int offset = 0;//外键数据偏移
            for (auto &attrName: attrNames) {
                int attr_id = getAttrIDByName(tableInfo, attrName);
                AttrInfo &attrInfo = tableInfo._attrs[attr_id];
                //从记录的对应位置拷贝到外键的对应位置
                memcpy(foreign + offset, data + attrInfo._offset, attrInfo._attrLength);
                offset += attrInfo._attrLength;
            }
            //检查主键文件是否插入成功，若成功说明外键值没有出现在参照表的主键中
            if (indexHandle1.insertEntry((BufType) foreign, rid, true, true)) {
                _indexManager->closeIndex(fileID1);
                _indexManager->closeIndex(fileID2);
                _indexManager->destroyIndex(tableName.c_str(), foreignAttrNames);
                cerr << "Foreign key value is not in the reference table!" << endl;
                delete[] data;
                delete[] foreign;
                delete[] attrTypes;
                delete[] attrLens;
                return false;
            }
            indexHandle2.insertEntry((BufType) foreign, rid, false, false);
            memset(data, 0, tableInfo._recordSize);
            memset(foreign, 0, foreignKeySize);
        }
        delete[] data;
        delete[] foreign;
    }
    //关闭主键文件和外键文件
    _indexManager->closeIndex(fileID1);
    _indexManager->closeIndex(fileID2);
    tableInfo._foreignKeyNames.push_back(foreignKeyName);
    tableInfo._foreignKeys.push_back(attrNames);
    tableInfo._references.push_back(reference);
    tableInfo._foreignKeyNum++;
    delete[] attrTypes;
    delete[] attrLens;
    return true;
}

bool SystemManager::dropForeign(const std::string &tableName, const std::string &foreignKeyName) {
    //检查表是否存在
    int table_id = getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    TableInfo &tableInfo = _tables[table_id];
    //检查外键是否存在
    int pos = -1;
    for (int i = 0; i < tableInfo._foreignKeyNum; i++) {
        if (foreignKeyName == tableInfo._foreignKeyNames[i]) {
            pos = i;
            break;
        }
    }
    if (pos == -1) {
        cerr << "Foreign key " + foreignKeyName + " does not exist!" << endl;
        return false;
    }
    vector<string> foreignAttrNames = vector<string>(tableInfo._foreignKeys[pos]);
    foreignAttrNames.emplace_back("foreign");
    _indexManager->destroyIndex(tableName.c_str(), foreignAttrNames);
    tableInfo._foreignKeyNames.erase(tableInfo._foreignKeyNames.begin() + pos);
    tableInfo._foreignKeys.erase(tableInfo._foreignKeys.begin() + pos);
    tableInfo._references.erase(tableInfo._references.begin() + pos);
    tableInfo._foreignKeyNum--;
    return true;
}

bool SystemManager::createUnique(const string &tableName, const vector<string> &attrNames) {
    //检查表是否存在
    int table_id = getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    //检查unique字段是否存在
    TableInfo &tableInfo = _tables[table_id];
    for (const auto &unique : tableInfo._uniques) {
        if (unique == attrNames) {
            cerr << "Columns are already unique!" << endl;
            return false;
        }
    }
    //检查对应列是否存在、非空
    for (auto &attrName : attrNames) {
        int attr_id = getAttrIDByName(tableInfo, attrName);
        if (attr_id == -1) {
            cerr << "Column " << attrName << " does not exist!" << endl;
            return false;
        }
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        if (!attrInfo._notNull) {
            cerr << "Column " << attrName << " must be not null!" << endl;
            return false;
        }
    }
    //以下根据记录建立unique
    int attrNum = (int) attrNames.size();
    auto *attrTypes = new AttrType[attrNum];
    int *attrLens = new int[attrNum];
    int attrLen = 0;//unique字段总大小，单位：字节
    for (int i = 0; i < attrNum; i++) {
        int attr_id = getAttrIDByName(tableInfo, attrNames[i]);
        AttrInfo &attrInfo = tableInfo._attrs[attr_id];
        attrTypes[i] = attrInfo._attrType;
        attrLens[i] = attrInfo._attrLength;
        attrLen += attrInfo._attrLength;
    }
    //创建unique文件
    vector<string> uniqueAttrNames = vector<string>(attrNames);
    uniqueAttrNames.emplace_back("unique");
    _indexManager->createIndex(tableName.c_str(), uniqueAttrNames, attrNum, attrLens, attrTypes);
    int fileID;
    _indexManager->openIndex(tableName.c_str(), uniqueAttrNames, fileID);
    IndexHandle indexHandle(_bufPageManager, fileID);
    RecordHandle recordHandle(_bufPageManager, _tableName2fileID[tableName]);
    //扫描每一条记录，插入一条unique数据
    if (recordHandle.openScan()) {
        RID rid;
        auto data = new char[tableInfo._recordSize];//记录数据
        auto unique = new char[attrLen];//unique数据
        while (recordHandle.getNextRecord(rid, (BufType)data)) {
            int offset = 0;//unique数据偏移
            for (auto &attrName : attrNames) {
                int attr_id = getAttrIDByName(tableInfo, attrName);
                AttrInfo &attrInfo = tableInfo._attrs[attr_id];
                //从记录的对应位置拷贝到unique文件的对应位置
                memcpy(unique + offset, data + attrInfo._offset, attrInfo._attrLength);
                offset += attrInfo._attrLength;
            }
            //检查是否出现重复，若重复则创建unique失败
            if (!indexHandle.insertEntry((BufType)unique, rid, true, false)) {
                _indexManager->closeIndex(fileID);
                _indexManager->destroyIndex(tableName.c_str(), uniqueAttrNames);
                cerr << "Repetitive unique keys!" << endl;
                delete[] data;
                delete[] unique;
                delete[] attrTypes;
                delete[] attrLens;
                return false;
            }
            memset(data, 0, tableInfo._recordSize);
            memset(unique, 0, attrLen);
        }
        delete[] data;
        delete[] unique;
    }
    //关闭unique文件
    _indexManager->closeIndex(fileID);
    tableInfo._uniqueNum++;
    tableInfo._uniques.push_back(attrNames);
    delete[] attrTypes;
    delete[] attrLens;
    return true;
}

void SystemManager::show(const std::string &tableName) {
    int table_id = getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return;
    }
    vector<int> headerLength({5, 4, 4, 7});
    TableInfo &tableInfo = _tables[table_id];
    for (auto &attrInfo : tableInfo._attrs) {
        headerLength[0] = max((int) attrInfo._attrName.length(), headerLength[0]);
        if (attrInfo._attrType == INTEGER) {
            headerLength[1] = max(3, headerLength[1]);
        } else if (attrInfo._attrType == FLOAT) {
            headerLength[1] = max(5, headerLength[1]);
        } else {
            headerLength[1] = max((int) to_string(attrInfo._attrLength).length() + 9, headerLength[1]);
        }
        if (attrInfo._notNull) headerLength[2] = max(2, headerLength[2]);
        else headerLength[2] = max(3, headerLength[2]);
        if (attrInfo._hasDefault) {
            if (attrInfo._defaultValue != nullptr) {
                if (attrInfo._attrType == INTEGER) {
                    headerLength[3] = max((int) to_string(*((int *) attrInfo._defaultValue)).length(), headerLength[3]);
                } else if (attrInfo._attrType == FLOAT) {
                    headerLength[3] = max((int) to_string(*((float *) attrInfo._defaultValue)).length(), headerLength[3]);
                } else {
                    headerLength[3] = max((int) strlen(((char *) attrInfo._defaultValue)), headerLength[3]);
                }
            } else headerLength[3] = max(4, headerLength[3]);
        } else headerLength[3] = max(4, headerLength[3]);
    }
    cout << "+" << setfill('-') << setw(headerLength[0] + 3) << "+";
    cout << setfill('-') << setw(headerLength[1] + 3) << "+";
    cout << setfill('-') << setw(headerLength[2] + 3) << "+";
    cout << setfill('-') << setw(headerLength[3] + 3) << "+";
    cout << setfill(' ') << endl;
    cout << "| " << setw(headerLength[0]) << "Field" << " | ";
    cout << setw(headerLength[1]) << "Type" << " | ";
    cout << setw(headerLength[2]) << "Null" << " | ";
    cout << setw(headerLength[3]) << "Default" << " |";
    cout << endl;
    cout << "+" << setfill('-') << setw(headerLength[0] + 3) << "+";
    cout << setfill('-') << setw(headerLength[1] + 3) << "+";
    cout << setfill('-') << setw(headerLength[2] + 3) << "+";
    cout << setfill('-') << setw(headerLength[3] + 3) << "+";
    cout << setfill(' ') << endl;
    for (auto &attrInfo : tableInfo._attrs) {
        cout << "| " << setw(headerLength[0]) << attrInfo._attrName << " | ";
        if (attrInfo._attrType == INTEGER) {
            cout << setw(headerLength[1]) << "INT" << " | ";
        } else if (attrInfo._attrType == FLOAT) {
            cout << setw(headerLength[1]) << "FLOAT" << " | ";
        } else {
            cout << setw(headerLength[1]) << "VARCHAR(" + to_string(attrInfo._attrLength) + ")" << " | ";
        }
        if (attrInfo._notNull) cout << setw(headerLength[2]) << "NO" << " | ";
        else cout << setw(headerLength[2]) << "YES" << " | ";
        if (attrInfo._hasDefault) {
            if (attrInfo._defaultValue != nullptr) {
                if (attrInfo._attrType == INTEGER) {
                    cout << setw(headerLength[3]) << *((int *) attrInfo._defaultValue) << " |" << endl;
                } else if (attrInfo._attrType == FLOAT) {
                    cout << setw(headerLength[3]) << *((float *) attrInfo._defaultValue) << " |" << endl;
                } else {
                    cout << setw(headerLength[3]) << ((char *) attrInfo._defaultValue) << " |" << endl;
                }
            } else cout << setw(headerLength[3]) << "NULL" << " |" << endl;
        } else cout << setw(headerLength[3]) << "NULL" << " |" << endl;
    }
    cout << "+" << setfill('-') << setw(headerLength[0] + 3) << "+";
    cout << setfill('-') << setw(headerLength[1] + 3) << "+";
    cout << setfill('-') << setw(headerLength[2] + 3) << "+";
    cout << setfill('-') << setw(headerLength[3] + 3) << "+";
    cout << setfill(' ') << endl;
    if (!tableInfo._primaryKeys.empty()) {
        cout << "PRIMARY KEY (" << tableInfo._primaryKeys[0];
        for (int i = 1; i < tableInfo._primaryKeys.size(); i++) {
            cout << ", " << tableInfo._primaryKeys[i];
        }
        cout << ");" << endl;
    }
    if (!tableInfo._foreignKeyNames.empty()) {
        for (int i = 0; i < tableInfo._foreignKeyNames.size(); i++) {
            cout << "FOREIGN KEY "<< tableInfo._foreignKeyNames[i] << "(" ;
            cout << tableInfo._foreignKeys[i][0];
            for (int j = 1; j < tableInfo._foreignKeys[i].size(); j++) {
                cout << ", " << tableInfo._foreignKeys[i][j];
            }
            cout << ") REFERENCES " << tableInfo._references[i] << "(";
            int reference_table_id = getTableIDByName(tableInfo._references[i]);
            TableInfo &refTableInfo = _tables[reference_table_id];
            cout << refTableInfo._primaryKeys[0];
            for (int j = 1; j < refTableInfo._primaryKeys.size(); j++) {
                cout << ", " << refTableInfo._primaryKeys[j];
            }
            cout << ");" << endl;
        }
    }
    if (!tableInfo._uniques.empty()) {
        for (auto & unique : tableInfo._uniques) {
            cout << "UNIQUE (" << unique[0];
            for (int j = 1; j < unique.size(); j++) {
                cout << ", " << unique[j];
            }
            cout << ");" << endl;
        }
    }
    if (!tableInfo._indexes.empty()) {
        for (auto & index : tableInfo._indexes) {
            cout << "INDEX (" << index[0];
            for (int j = 1; j < index.size(); j++) {
                cout << ", " << index[j];
            }
            cout << ");" << endl;
        }
    }
}

void SystemManager::showDBNames() {
    for (const auto &dbName: _dbNames) {
        cout << dbName << endl;
    }
}

void SystemManager::showTableNames() {
    for (const auto &table: _tables) {
        cout << table._tableName << endl;
    }
}

void SystemManager::showIndexNames() {
    for (const auto &table: _tables) {
        if (!table._indexes.empty()) {
            for (auto & index : table._indexes) {
                cout << "(" << index[0];
                for (int j = 1; j < index.size(); j++) {
                    cout << ", " << index[j];
                }
                cout << ")" << endl;
            }
        }
    }
}