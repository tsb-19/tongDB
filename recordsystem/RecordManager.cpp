#include "RecordSystem.h"
#include <cmath>
#include <cstring>

RecordManager::RecordManager(BufPageManager *bufPageManager, FileManager *fileManager) {
    _bufPageManager = bufPageManager;
    _fileManager = fileManager;
}

bool RecordManager::createFile(const char *fileName, int recordSize) {
    if (recordSize > PAGE_SIZE / 2) return false;
    if (!_fileManager->createFile(fileName)) return false;
    int availableSize = PAGE_SIZE - nextPageOffset;//所有的可用空间8188B
    int recordCount = availableSize * 8 / (1 + recordSize * 8);
    int bitmapSize = ceil(recordCount / 8.0);
    while (bitmapSize + recordCount * recordSize + recordSize <= availableSize) recordCount++;
    RecordHeader header{
        ._recordSize = recordSize,
        ._recordCount = recordCount,
        ._bitmapSize = bitmapSize,
        ._firstEmptyPage = 0,
        ._pageNumber = 0
    };
    int index, fileID;
    if (!_fileManager->openFile(fileName, fileID)) return false;
    BufType b = _bufPageManager->getPage(fileID, 0, index);
    memset(b, 0, PAGE_SIZE);
    memcpy(b, &header, sizeof(RecordHeader));
    _bufPageManager->markDirty(index);
    _bufPageManager->writeBack(index);
    return (!_fileManager->closeFile(fileID));
}

bool RecordManager::destroyFile(const char *fileName) {
    //将缓存全部写回，删除文件
    _bufPageManager->close();
    return (!remove(fileName));
}

bool RecordManager::openFile(const char *fileName, int &fileID) {
    return _fileManager->openFile(fileName, fileID);
}

bool RecordManager::closeFile(int fileID) {
    //将缓存全部写回，关闭文件
    _bufPageManager->close();
    return (!_fileManager->closeFile(fileID));
}