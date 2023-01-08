#include "IndexSystem.h"
#include <cstring>

IndexManager::IndexManager(BufPageManager *bufPageManager, FileManager *fileManager) {
    _bufPageManager = bufPageManager;
    _fileManager = fileManager;
}

bool IndexManager::createIndex(const char *fileName, const std::vector<std::string> &attrNames, int attrNum, const int *attrLens, const AttrType *attrTypes) {
    std::string indexName = std::string(fileName, fileName + strlen(fileName));
    for (const auto &attrName: attrNames) {
        indexName += "." + attrName;
    }
    if (!_fileManager->createFile(indexName.c_str())) return false;
    int fileID;
    if (!_fileManager->openFile(indexName.c_str(), fileID)) return false;
    int attrLen = 0;
    for (int i = 0; i < attrNum; i++) {
        attrLen += attrLens[i];
    }
    int maxChildNum = (int)((PAGE_SIZE - 6 * sizeof(int)) / (attrLen + sizeof(int) + sizeof(RID)));
    int keyStart = 6 * sizeof(int);
    int childStart = keyStart + maxChildNum * attrLen;
    int ridStart = (int)(childStart + maxChildNum * sizeof(int));
    IndexHeader header{
        ._attrNum = attrNum,
        ._attrLen = attrLen,
        ._root = 1,
        ._maxChildNum = maxChildNum,
        ._firstEmptyPage = 0,
        ._pageNumber = 1,
        ._keyStart = keyStart,
        ._childStart = childStart,
        ._ridStart = ridStart
    };
    int index;
    BufType b = _bufPageManager->getPage(fileID, 0, index);
    memset(b, 0, PAGE_SIZE);
    memcpy(b, &header, sizeof(IndexHeader));
    memcpy((char *) b + sizeof(IndexHeader), attrTypes, attrNum * sizeof(AttrType));
    memcpy((char *) b + sizeof(IndexHeader) + attrNum * sizeof(AttrType), attrLens, attrNum * sizeof(int));
    _bufPageManager->markDirty(index);
    _bufPageManager->writeBack(index);
    int node[6] = {1, 0, 0, 0, 0, 0};
    b = _bufPageManager->getPage(fileID, 1, index);
    memset(b, 0, PAGE_SIZE);
    memcpy(b, node, 6 * sizeof(int));
    _bufPageManager->markDirty(index);
    _bufPageManager->writeBack(index);
    return (!_fileManager->closeFile(fileID));
}

bool IndexManager::destroyIndex(const char *fileName, const std::vector<std::string> &attrNames) {
    //将缓存全部写回，删除文件
    _bufPageManager->close();
    std::string indexName = std::string(fileName, fileName + strlen(fileName));
    for (const auto &attrName: attrNames) {
        indexName += "." + attrName;
    }
    return (!remove(indexName.c_str()));
}

bool IndexManager::openIndex(const char *fileName, const std::vector<std::string> &attrNames, int &fileID) {
    std::string indexName = std::string(fileName, fileName + strlen(fileName));
    for (const auto &attrName: attrNames) {
        indexName += "." + attrName;
    }
    return _fileManager->openFile(indexName.c_str(), fileID);
}

bool IndexManager::closeIndex(int fileID) {
    //将缓存全部写回，关闭文件
    _bufPageManager->close();
    return (!_fileManager->closeFile(fileID));
}