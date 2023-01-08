#include "RecordSystem.h"
#include <cstring>

//页面格式：| bitmap | nextFreePage | records |

int RecordHandle::getFirstZeroBit(BufType b, int size) const {
    for (int i = 0; i < size; i++) {
        if (!(b[i >> 5] & (1u << (i & 31)))) return i;
    }
    return -1;
}

void RecordHandle::refreshHeader() const {
    int index;
    BufType b = _bufPageManager->getPage(_fileID, 0, index);
    memcpy(b, &_header, sizeof(RecordHeader));
    _bufPageManager->markDirty(index);
    _bufPageManager->writeBack(index);
}

RecordHandle::RecordHandle(BufPageManager *bufPageManager, int fileID) {
    _bufPageManager = bufPageManager;
    _fileID = fileID;
    int index;
    BufType b = _bufPageManager->getPage(_fileID, 0, index);
    _bufPageManager->access(index);
    memcpy(&_header, b, sizeof(RecordHeader));
}

void RecordHandle::getRecord(const RID &rid, BufType data) {
    int index;
    BufType b = _bufPageManager->getPage(_fileID, rid.getPageNum(), index);
    _bufPageManager->access(index);
    char *start = (char *) b + (_header._recordSize * rid.getSlotNum() + _header._bitmapSize + nextPageOffset);
    memcpy(data, start, _header._recordSize);
}

bool RecordHandle::insertRecord(BufType data, RID &rid) {
    if (_bufPageManager == nullptr) return false;
    int index;
    BufType b;
    //检查是否有空闲页
    if (_header._firstEmptyPage != 0) {
        b = _bufPageManager->getPage(_fileID, _header._firstEmptyPage, index);
    } else {
        //分配新的空闲页
        _header._pageNumber++;
        _header._firstEmptyPage = _header._pageNumber;
        b = _bufPageManager->getPage(_fileID, _header._firstEmptyPage, index);
        memset(b, 0, PAGE_SIZE);
        refreshHeader();
    }
    int pageNum = _header._firstEmptyPage;
    _bufPageManager->access(index);
    int slotNum = getFirstZeroBit(b, _header._recordCount);
    rid.setPageNum(pageNum);
    rid.setSlotNum(slotNum);
    _bufPageManager->markDirty(index);
    b[slotNum >> 5] |= (1u << (slotNum & 31));//标记位图
    char *start = (char *) b + (_header._recordSize * slotNum + _header._bitmapSize + nextPageOffset);
    memcpy(start, data, _header._recordSize);
    //如果插入后当前页面已满，需修改页头信息
    if (getFirstZeroBit(b, _header._recordCount) == -1) {
        memcpy(&_header._firstEmptyPage, (char *) b + _header._bitmapSize, nextPageOffset);
        memset((char *) b + _header._bitmapSize, 0, nextPageOffset);
        refreshHeader();
    }
    _bufPageManager->writeBack(index);
    return true;
}

bool RecordHandle::deleteRecord(const RID &rid) {
    if (_bufPageManager == nullptr) return false;
    if (rid.getPageNum() <= 0 || rid.getPageNum() > _header._pageNumber) return false;
    if (rid.getSlotNum() < 0 || rid.getSlotNum() >= _header._recordCount) return false;
    int index;
    int pageNum = rid.getPageNum();
    int slotNum = rid.getSlotNum();
    BufType b = _bufPageManager->getPage(_fileID, pageNum, index);
    _bufPageManager->access(index);
    //位图为1才是有效的删除
    if (b[slotNum >> 5] & (1u << (slotNum & 31))) {
        _bufPageManager->markDirty(index);
        char *start = (char *) b + (_header._recordSize * slotNum + _header._bitmapSize + nextPageOffset);
        memset(start, 0, _header._recordSize);
        //如果原来页面已满，则需要修改页头
        if (getFirstZeroBit(b, _header._recordCount) == -1) {
            memcpy((char *) b + _header._bitmapSize, &_header._firstEmptyPage, nextPageOffset);
            memcpy(&_header._firstEmptyPage, &pageNum, nextPageOffset);
            refreshHeader();
        }
        b[slotNum >> 5] &= ~(1u << (slotNum & 31));//修改位图
        _bufPageManager->writeBack(index);
    } else return false;
    return true;
}

bool RecordHandle::updateRecord(const RID &rid, BufType data) {
    if (_bufPageManager == nullptr) return false;
    if (rid.getPageNum() <= 0 || rid.getPageNum() > _header._pageNumber) return false;
    if (rid.getSlotNum() < 0 || rid.getSlotNum() >= _header._recordCount) return false;
    int index;
    BufType b = _bufPageManager->getPage(_fileID, rid.getPageNum(), index);
    _bufPageManager->access(index);
    //检查位图是否为1
    if (!(b[rid.getSlotNum() >> 5] & (1u << (rid.getSlotNum() & 31)))) return false;
    _bufPageManager->markDirty(index);
    char *start = (char *) b + (_header._recordSize * rid.getSlotNum() + _header._bitmapSize + nextPageOffset);
    memcpy(start, data, _header._recordSize);
    _bufPageManager->writeBack(index);
    return true;
}

bool RecordHandle::openScan() {
    int pageNum = 1, slotNum = 0;
    while (true) {
        if (pageNum > _header._pageNumber) return false;//说明没有记录
        int index;
        BufType b = _bufPageManager->getPage(_fileID, pageNum, index);
        _bufPageManager->access(index);
        while (slotNum < _header._recordCount && !(b[slotNum >> 5] & (1u << (slotNum & 31)))) slotNum++;
        if (slotNum == _header._recordCount) {
            //当前页面没有找到记录，在下一页面继续扫描
            slotNum = 0;
            pageNum++;
        } else {
            //找到第一条记录位置
            _rid.setPageNum(pageNum);
            _rid.setSlotNum(slotNum);
            break;
        }
    }
    return true;
}

bool RecordHandle::getNextRecord(RID &rid, BufType data) {
    //_rid记录当前扫描位置，直接返回结果，将_rid置为下一条记录位置
    int pageNum = _rid.getPageNum(), slotNum = _rid.getSlotNum();
    if (pageNum > _header._pageNumber) return false;//已经扫描所有页面，返回false
    rid.setPageNum(pageNum);
    rid.setSlotNum(slotNum);
    int index;
    BufType b = _bufPageManager->getPage(_fileID, pageNum, index);
    _bufPageManager->access(index);
    char *start = (char *) b + (_header._recordSize * slotNum + _header._bitmapSize + nextPageOffset);
    memcpy(data, start, _header._recordSize);
    slotNum++;
    while (true) {
        while (slotNum < _header._recordCount && !(b[slotNum >> 5] & (1u << (slotNum & 31)))) slotNum++;
        if (slotNum == _header._recordCount) {
            slotNum = 0;
            pageNum++;
            if (pageNum <= _header._pageNumber) {
                b = _bufPageManager->getPage(_fileID, pageNum, index);
                _bufPageManager->access(index);
            } else break;//扫描完全部记录
        } else break;//找到下一条记录
    }
    //更新_rid
    _rid.setPageNum(pageNum);
    _rid.setSlotNum(slotNum);
    return true;
}