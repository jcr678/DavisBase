package storage;

import common.CatalogDB;
import common.Constants;
import common.Utils;
import datatypes.*;
import datatypes.base.DT;
import datatypes.base.DT_Numeric;
import errors.InternalException;
import helpers.UpdateStatementHelper;
import storage.model.DataRecord;
import storage.model.InternalCondition;
import storage.model.Page;
import storage.model.PointerRecord;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by dakle on 9/4/17.
 */
public class StorageManager {

    public boolean databaseExists(String databaseName) {
        File databaseDir = new File(Utils.getDatabasePath(databaseName));
        return  databaseDir.exists();
    }

    public boolean createTable(String databaseName, String tableName) throws InternalException {
        try {
            File dirFile = new File(Utils.getDatabasePath(databaseName));
            if (!dirFile.exists()) {
                dirFile.mkdir();
            }
            File file = new File(Utils.getDatabasePath(databaseName) + "/" + tableName);
            if (file.exists()) {
                return false;
            }
            if (file.createNewFile()) {
                RandomAccessFile randomAccessFile;
                Page<DataRecord> page = Page.createNewEmptyPage(new DataRecord());
                randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(Page.PAGE_SIZE);
                boolean isTableCreated = writePageHeader(randomAccessFile, page);
                randomAccessFile.close();
                return isTableCreated;
            }
            return false;
        } catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    /***
     * Checks if the table exists in the database.
     * @param databaseName
     * @param tableName
     * @return True if the table exists else False.
     */
    public boolean checkTableExists(String databaseName, String tableName) {
        boolean databaseExists = this.databaseExists(databaseName);
        boolean fileExists = new File(Utils.getDatabasePath(databaseName) + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION).exists();

        return (databaseExists && fileExists);
    }

    public boolean writeRecord(String databaseName, String tableName, DataRecord record) throws InternalException {
        RandomAccessFile randomAccessFile;
        try {
            File file = new File(Utils.getDatabasePath(databaseName) + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                randomAccessFile = new RandomAccessFile(file, "rw");
                Page page = getPage(randomAccessFile, record, 0);
                if (page == null) return false;
                if (!checkSpaceRequirements(page, record)) {
                    int pageCount = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                    switch (pageCount) {
                        case 1:
                            PointerRecord pointerRecord = splitPage(randomAccessFile, page, record, 1, 2);
                            Page<PointerRecord> pointerRecordPage = Page.createNewEmptyPage(pointerRecord);
                            pointerRecordPage.setPageNumber(0);
                            pointerRecordPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                            pointerRecordPage.setNumberOfCells((byte) 1);
                            pointerRecordPage.setStartingAddress((short) (pointerRecordPage.getStartingAddress() - pointerRecord.getSize()));
                            pointerRecordPage.setRightNodeAddress(2);
                            pointerRecordPage.getRecordAddressList().add((short) (pointerRecordPage.getStartingAddress() + 1));
                            pointerRecord.setPageNumber(pointerRecordPage.getPageNumber());
                            pointerRecord.setOffset((short) (pointerRecordPage.getStartingAddress() + 1));
                            this.writePageHeader(randomAccessFile, pointerRecordPage);
                            this.writeRecord(randomAccessFile, pointerRecord);
                            break;

                        default:
                            if(pageCount > 1) {
                                PointerRecord pointerRecord1 = splitPage(randomAccessFile, readPageHeader(randomAccessFile, 0), record);
                                if(pointerRecord1 != null && pointerRecord1.getLeftPageNumber() != -1)  {
                                    Page<PointerRecord> rootPage = Page.createNewEmptyPage(pointerRecord1);
                                    rootPage.setPageNumber(0);
                                    rootPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                                    rootPage.setNumberOfCells((byte) 1);
                                    rootPage.setStartingAddress((short)(rootPage.getStartingAddress() - pointerRecord1.getSize()));
                                    rootPage.setRightNodeAddress(pointerRecord1.getPageNumber());
                                    rootPage.getRecordAddressList().add((short) (rootPage.getStartingAddress() + 1));
                                    pointerRecord1.setOffset((short) (rootPage.getStartingAddress() + 1));
                                    this.writePageHeader(randomAccessFile, rootPage);
                                    this.writeRecord(randomAccessFile, pointerRecord1);
                                }
                            }
                            break;
                    }
                    UpdateStatementHelper.incrementRowCount(databaseName, tableName);
                    randomAccessFile.close();
                    return true;
                }
                short address = (short) getAddress(file, record.getRowId(), page.getPageNumber());
                page.setNumberOfCells((byte)(page.getNumberOfCells() + 1));
                page.setStartingAddress((short) (page.getStartingAddress() - record.getSize() - record.getHeaderSize()));
                if(address == page.getRecordAddressList().size())
                    page.getRecordAddressList().add((short)(page.getStartingAddress() + 1));
                else
                    page.getRecordAddressList().add(address, (short)(page.getStartingAddress() + 1));
                record.setPageLocated(page.getPageNumber());
                record.setOffset((short) (page.getStartingAddress() + 1));
                this.writePageHeader(randomAccessFile, page);
                this.writeRecord(randomAccessFile, record);
                randomAccessFile.close();
            } else {
                Utils.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
            }
            return true;
        } catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private boolean checkSpaceRequirements(Page page, DataRecord record) {
        if (page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSize() + record.getHeaderSize() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    private boolean checkSpaceRequirements(Page page, PointerRecord record) {
        if(page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSize() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    private PointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, DataRecord record, int pageNumber1, int pageNumber2) throws InternalException {
        try {
            if (page != null && record != null) {
                int location;
                PointerRecord pointerRecord = new PointerRecord();
                if (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType());
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));
                if (location == page.getNumberOfCells()) {
                    Page<DataRecord> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setNumberOfCells(page.getNumberOfCells());
                    page1.setRightNodeAddress(pageNumber2);
                    page1.setStartingAddress(page.getStartingAddress());
                    page1.setRecordAddressList(page.getRecordAddressList());
                    this.writePageHeader(randomAccessFile, page1);
                    List<DataRecord> records = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, page.getNumberOfCells(), page1.getPageNumber(), record);
                    for (DataRecord object : records) {
                        this.writeRecord(randomAccessFile, object);
                    }
                    Page<DataRecord> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    page2.setNumberOfCells((byte) 1);
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    page2.setStartingAddress((short) (page2.getStartingAddress() - record.getSize() - record.getHeaderSize()));
                    page2.getRecordAddressList().add((short) (page2.getStartingAddress() + 1));
                    this.writePageHeader(randomAccessFile, page2);
                    record.setPageLocated(page2.getPageNumber());
                    record.setOffset((short) (page2.getStartingAddress() + 1));
                    this.writeRecord(randomAccessFile, record);
                    pointerRecord.setKey(record.getRowId());
                } else {
                    boolean isFirst = false;
                    if (location < (page.getRecordAddressList().size() / 2)) {
                        isFirst = true;
                    }
                    randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));

                    //Page 1
                    Page<DataRecord> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setPageNumber(pageNumber1);
                    List<DataRecord> leftRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, (byte) (page.getNumberOfCells() / 2), page1.getPageNumber(), record);
                    if (isFirst)
                        leftRecords.add(location, record);
                    page1.setNumberOfCells((byte) leftRecords.size());
                    int index = 0;
                    short offset = (short) (Page.PAGE_SIZE - 1);
                    for (DataRecord dataRecord : leftRecords) {
                        index++;
                        offset = (short) (Page.PAGE_SIZE - ((dataRecord.getSize() + dataRecord.getHeaderSize()) * index));
                        dataRecord.setOffset(offset);
                        page1.getRecordAddressList().add(offset);
                    }
                    page1.setStartingAddress((short) (offset + 1));
                    page1.setRightNodeAddress(pageNumber2);
                    this.writePageHeader(randomAccessFile, page1);
                    for(DataRecord dataRecord : leftRecords) {
                        this.writeRecord(randomAccessFile, dataRecord);
                    }

                    //Page 2
                    Page<DataRecord> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    List<DataRecord> rightRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) ((page.getNumberOfCells() / 2) + 1), page.getNumberOfCells(), pageNumber2, record);
                    if(!isFirst) {
                        int position = (location - (page.getRecordAddressList().size() / 2) + 1);
                        if(position >= rightRecords.size())
                            rightRecords.add(record);
                        else
                            rightRecords.add(position, record);
                    }
                    page2.setNumberOfCells((byte) rightRecords.size());
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    pointerRecord.setKey(rightRecords.get(0).getRowId());
                    index = 0;
                    offset = (short) (Page.PAGE_SIZE - 1);
                    for(DataRecord dataRecord : rightRecords) {
                        index++;
                        offset = (short) (Page.PAGE_SIZE - ((dataRecord.getSize() + dataRecord.getHeaderSize()) * index));
                        dataRecord.setOffset(offset);
                        page2.getRecordAddressList().add(offset);
                    }
                    page2.setStartingAddress((short) (offset + 1));
                    this.writePageHeader(randomAccessFile, page2);
                    for(DataRecord dataRecord : rightRecords) {
                        this.writeRecord(randomAccessFile, dataRecord);
                    }
                }
                pointerRecord.setLeftPageNumber(pageNumber1);
                return pointerRecord;
            }
        } catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return null;
    }

    private PointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, DataRecord record) throws InternalException {
        try {
            if (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                int pageNumber = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
                Page newPage = this.readPageHeader(randomAccessFile, pageNumber);
                PointerRecord pointerRecord = splitPage(randomAccessFile, newPage, record);
                if (pointerRecord.getPageNumber() == -1)
                    return pointerRecord;
                if (checkSpaceRequirements(page, pointerRecord)) {
                    int location = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE, true);
                    page.setNumberOfCells((byte) (page.getNumberOfCells() + 1));
                    page.setStartingAddress((short) (page.getStartingAddress() - pointerRecord.getSize()));
                    page.getRecordAddressList().add(location, (short) (page.getStartingAddress() + 1));
                    page.setRightNodeAddress(pointerRecord.getPageNumber());
                    pointerRecord.setPageNumber(page.getPageNumber());
                    pointerRecord.setOffset((short) (page.getStartingAddress() + 1));
                    this.writePageHeader(randomAccessFile, page);
                    this.writeRecord(randomAccessFile, pointerRecord);
                    return new PointerRecord();
                } else {
                    int newPageNumber;
                    try {
                        newPageNumber = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                    page.setRightNodeAddress(pointerRecord.getPageNumber());
                    this.writePageHeader(randomAccessFile, page);
                    PointerRecord pointerRecord1;
                    pointerRecord1 = splitPage(randomAccessFile, page, pointerRecord, page.getPageNumber(), newPageNumber);
                    return pointerRecord1;
                }
            } else if (page.getPageType() == Page.LEAF_TABLE_PAGE) {
                int newPageNumber;
                try {
                    newPageNumber = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
                PointerRecord pointerRecord = splitPage(randomAccessFile, page, record, page.getPageNumber(), newPageNumber);
                if (pointerRecord != null)
                    pointerRecord.setPageNumber(newPageNumber);
                return pointerRecord;
            }
            return null;
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private PointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, PointerRecord record, int pageNumber1, int pageNumber2) throws InternalException {
        try {
            if (page != null && record != null) {
                int location;
                boolean isFirst = false;

                PointerRecord pointerRecord;
                if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getKey(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType(), true);
                if (location < (page.getRecordAddressList().size() / 2)) {
                    isFirst = true;
                }

                if(pageNumber1 == 0) {
                    pageNumber1 = pageNumber2;
                    pageNumber2++;
                }
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));

                //Page 1
                Page<PointerRecord> page1 = new Page<>(pageNumber1);
                page1.setPageType(page.getPageType());
                page1.setPageNumber(pageNumber1);
                List<PointerRecord> leftRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, (byte) (page.getNumberOfCells() / 2), page1.getPageNumber(), record);
                if (isFirst)
                    leftRecords.add(location, record);
                pointerRecord = leftRecords.get(leftRecords.size() - 1);
                pointerRecord.setPageNumber(pageNumber2);
                leftRecords.remove(leftRecords.size() - 1);
                page1.setNumberOfCells((byte) leftRecords.size());
                int index = 0;
                short offset = (short) (Page.PAGE_SIZE - 1);
                for (PointerRecord pointerRecord1 : leftRecords) {
                    index++;
                    offset = (short) (Page.PAGE_SIZE - (pointerRecord1.getSize() * index));
                    pointerRecord1.setOffset(offset);
                    page1.getRecordAddressList().add(offset);
                }
                page1.setStartingAddress((short) (offset + 1));
                page1.setRightNodeAddress(pointerRecord.getLeftPageNumber());
                this.writePageHeader(randomAccessFile, page1);
                for(PointerRecord pointerRecord1 : leftRecords) {
                    this.writeRecord(randomAccessFile, pointerRecord1);
                }

                //Page 2
                Page<PointerRecord> page2 = new Page<>(pageNumber2);
                page2.setPageType(page.getPageType());
                List<PointerRecord> rightRecords = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) ((page.getNumberOfCells() / 2) + 1), page.getNumberOfCells(), pageNumber2, record);
                if(!isFirst) {
                    int position = (location - (page.getRecordAddressList().size() / 2) + 1);
                    if(position >= rightRecords.size())
                        rightRecords.add(record);
                    else
                        rightRecords.add(position, record);
                }
                page2.setNumberOfCells((byte) rightRecords.size());
                page2.setRightNodeAddress(page.getRightNodeAddress());
                rightRecords.get(0).setLeftPageNumber(page.getRightNodeAddress());
                index = 0;
                offset = (short) (Page.PAGE_SIZE - 1);
                for(PointerRecord pointerRecord1 : rightRecords) {
                    index++;
                    offset = (short) (Page.PAGE_SIZE - (pointerRecord1.getSize() * index));
                    pointerRecord1.setOffset(offset);
                    page2.getRecordAddressList().add(offset);
                }
                page2.setStartingAddress((short) (offset + 1));
                this.writePageHeader(randomAccessFile, page2);
                for(PointerRecord pointerRecord1 : rightRecords) {
                    this.writeRecord(randomAccessFile, pointerRecord1);
                }
                pointerRecord.setPageNumber(pageNumber2);
                pointerRecord.setLeftPageNumber(pageNumber1);
                return pointerRecord;
            }
        } catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return null;
    }

    private <T> List<T> copyRecords(RandomAccessFile randomAccessFile, long pageStartAddress, List<Short> recordAddresses, byte startIndex, byte endIndex, int pageNumber, T object) throws InternalException {
        try {
            List<T> records = new ArrayList<>();
            byte numberOfRecords;
            byte[] serialTypeCodes;
            for (byte i = startIndex; i < endIndex; i++) {
                randomAccessFile.seek(pageStartAddress + recordAddresses.get(i));
                if (object.getClass().equals(PointerRecord.class)) {
                    PointerRecord record = new PointerRecord();
                    record.setPageNumber(pageNumber);
                    record.setOffset((short) (pageStartAddress + Page.PAGE_SIZE - 1 - (record.getSize() * (i - startIndex + 1))));
                    record.setLeftPageNumber(randomAccessFile.readInt());
                    record.setKey(randomAccessFile.readInt());
                    records.add(i - startIndex, (T) record);
                } else if (object.getClass().equals(DataRecord.class)) {
                    DataRecord record = new DataRecord();
                    record.setPageLocated(pageNumber);
                    record.setOffset(recordAddresses.get(i));
                    record.setSize(randomAccessFile.readShort());
                    record.setRowId(randomAccessFile.readInt());
                    numberOfRecords = randomAccessFile.readByte();
                    serialTypeCodes = new byte[numberOfRecords];
                    for (byte j = 0; j < numberOfRecords; j++) {
                        serialTypeCodes[j] = randomAccessFile.readByte();
                    }
                    for (byte j = 0; j < numberOfRecords; j++) {
                        switch (serialTypeCodes[j]) {
                            //case DT_TinyInt.nullSerialCode is overridden with DT_Text

                            case Constants.ONE_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Text(null));
                                break;

                            case Constants.TWO_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_SmallInt(randomAccessFile.readShort(), true));
                                break;

                            case Constants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Real(randomAccessFile.readFloat(), true));
                                break;

                            case Constants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Double(randomAccessFile.readDouble(), true));
                                break;

                            case Constants.TINY_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_TinyInt(randomAccessFile.readByte()));
                                break;

                            case Constants.SMALL_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_SmallInt(randomAccessFile.readShort()));
                                break;

                            case Constants.INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Int(randomAccessFile.readInt()));
                                break;

                            case Constants.BIG_INT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_BigInt(randomAccessFile.readLong()));
                                break;

                            case Constants.REAL_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Real(randomAccessFile.readFloat()));
                                break;

                            case Constants.DOUBLE_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Double(randomAccessFile.readDouble()));
                                break;

                            case Constants.DATE_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Date(randomAccessFile.readLong()));
                                break;

                            case Constants.DATE_TIME_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_DateTime(randomAccessFile.readLong()));
                                break;

                            case Constants.TEXT_SERIAL_TYPE_CODE:
                                record.getColumnValueList().add(new DT_Text(""));
                                break;

                            default:
                                if (serialTypeCodes[j] > Constants.TEXT_SERIAL_TYPE_CODE) {
                                    byte length = (byte) (serialTypeCodes[j] - Constants.TEXT_SERIAL_TYPE_CODE);
                                    char[] text = new char[length];
                                    for (byte k = 0; k < length; k++) {
                                        text[k] = (char) randomAccessFile.readByte();
                                    }
                                    record.getColumnValueList().add(new DT_Text(new String(text)));
                                }
                                break;

                        }
                    }
                    records.add(i - startIndex, (T) record);
                }
            }
            return records;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private Page getPage(RandomAccessFile randomAccessFile, DataRecord record, int pageNumber) throws InternalException {
        try {
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if (page.getPageType() == Page.LEAF_TABLE_PAGE) {
                return page;
            }
            pageNumber = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
            if (pageNumber == -1) return null;
            return getPage(randomAccessFile, record, pageNumber);
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private int getAddress(File file, int rowId, int pageNumber) throws InternalException {
        int location = -1;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                location = binarySearch(randomAccessFile, rowId, page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.LEAF_TABLE_PAGE);
                randomAccessFile.close();
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return location;
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType) throws InternalException {
        return binarySearch(randomAccessFile, key, numberOfRecords, seekPosition, pageType, false);
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType, boolean literalSearch) throws InternalException {
        try {
            int start = 0, end = numberOfRecords;
            int mid;
            int pageNumber = -1;
            int rowId;
            short address;

            while(true) {
                if(start > end || start == numberOfRecords) {
                    if(pageType == Page.LEAF_TABLE_PAGE || literalSearch)
                        return start > numberOfRecords ? numberOfRecords : start;
                    if(pageType == Page.INTERIOR_TABLE_PAGE) {
                        if (end < 0)
                            return pageNumber;
                        randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + 4);
                        return randomAccessFile.readInt();
                    }
                }
                mid = (start + end) / 2;
                randomAccessFile.seek(seekPosition + (Short.BYTES * mid));
                address = randomAccessFile.readShort();
                randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + address);
                if (pageType == Page.LEAF_TABLE_PAGE) {
                    randomAccessFile.readShort();
                    rowId = randomAccessFile.readInt();
                    if (rowId == key) return mid;
                    if (rowId > key) {
                        end = mid - 1;
                    } else {
                        start = mid + 1;
                    }
                } else if (pageType == Page.INTERIOR_TABLE_PAGE) {
                    pageNumber = randomAccessFile.readInt();
                    rowId = randomAccessFile.readInt();
                    if (rowId > key) {
                        end = mid - 1;
                    } else {
                        start = mid + 1;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private Page readPageHeader(RandomAccessFile randomAccessFile, int pageNumber) throws InternalException {
        try {
            Page page;
            randomAccessFile.seek(Page.PAGE_SIZE * pageNumber);
            byte pageType = randomAccessFile.readByte();
            if (pageType == Page.INTERIOR_TABLE_PAGE) {
                page = new Page<PointerRecord>();
            } else {
                page = new Page<DataRecord>();
            }
            page.setPageType(pageType);
            page.setPageNumber(pageNumber);
            page.setNumberOfCells(randomAccessFile.readByte());
            page.setStartingAddress(randomAccessFile.readShort());
            page.setRightNodeAddress(randomAccessFile.readInt());
            for (byte i = 0; i < page.getNumberOfCells(); i++) {
                page.getRecordAddressList().add(randomAccessFile.readShort());
            }
            return page;
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private boolean writePageHeader(RandomAccessFile randomAccessFile, Page page) throws InternalException {
        try {
            randomAccessFile.seek(page.getPageNumber() * Page.PAGE_SIZE);
            randomAccessFile.writeByte(page.getPageType());
            randomAccessFile.writeByte(page.getNumberOfCells());
            randomAccessFile.writeShort(page.getStartingAddress());
            randomAccessFile.writeInt(page.getRightNodeAddress());
            for (Object offset : page.getRecordAddressList()) {
                randomAccessFile.writeShort((short) offset);
            }
            return true;
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, DataRecord record) throws InternalException {
        try {
            randomAccessFile.seek((record.getPageLocated() * Page.PAGE_SIZE) + record.getOffset());
            randomAccessFile.writeShort(record.getSize());
            randomAccessFile.writeInt(record.getRowId());
            randomAccessFile.writeByte((byte) record.getColumnValueList().size());
            randomAccessFile.write(record.getSerialTypeCodes());
            for (Object object : record.getColumnValueList()) {
                switch (Utils.resolveClass(object)) {
                    case Constants.TINYINT:
                        randomAccessFile.writeByte(((DT_TinyInt) object).getValue());
                        break;

                    case Constants.SMALLINT:
                        randomAccessFile.writeShort(((DT_SmallInt) object).getValue());
                        break;

                    case Constants.INT:
                        randomAccessFile.writeInt(((DT_Int) object).getValue());
                        break;

                    case Constants.BIGINT:
                        randomAccessFile.writeLong(((DT_BigInt) object).getValue());
                        break;

                    case Constants.REAL:
                        randomAccessFile.writeFloat(((DT_Real) object).getValue());
                        break;

                    case Constants.DOUBLE:
                        randomAccessFile.writeDouble(((DT_Double) object).getValue());
                        break;

                    case Constants.DATE:
                        randomAccessFile.writeLong(((DT_Date) object).getValue());
                        break;

                    case Constants.DATETIME:
                        randomAccessFile.writeLong(((DT_DateTime) object).getValue());
                        break;

                    case Constants.TEXT:
                        if (((DT_Text) object).getValue() != null)
                            randomAccessFile.writeBytes(((DT_Text) object).getValue());
                        break;

                    default:
                        break;
                }
            }
        } catch (ClassCastException e) {
            throw new InternalException(InternalException.INVALID_DATATYPE_EXCEPTION);
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return true;
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, PointerRecord record) throws InternalException {
        try {
            randomAccessFile.seek((record.getPageNumber() * Page.PAGE_SIZE) + record.getOffset());
            randomAccessFile.writeInt(record.getLeftPageNumber());
            randomAccessFile.writeInt(record.getKey());
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return true;
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, InternalCondition condition, boolean getOne) throws InternalException {
        return findRecord(databaseName, tableName, condition,null, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, InternalCondition condition, List<Byte> selectionColumnIndexList, boolean getOne) throws InternalException {
        List<InternalCondition> conditionList = new ArrayList<>();
        if(condition != null)
            conditionList.add(condition);
        return findRecord(databaseName, tableName, conditionList, selectionColumnIndexList, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<InternalCondition> conditionList, boolean getOne) throws InternalException {
        return findRecord(databaseName, tableName, conditionList, null, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<InternalCondition> conditionList, List<Byte> selectionColumnIndexList, boolean getOne) throws InternalException {
        try {
            File file = new File(Utils.getDatabasePath(databaseName) + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                if (conditionList != null) {
                    Page page = getFirstPage(file);
                    DataRecord record;
                    List<DataRecord> matchRecords = new ArrayList<>();
                    boolean isMatch = false;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Object offset : page.getRecordAddressList()) {
                            isMatch = true;
                            record = getDataRecord(randomAccessFile, page.getPageNumber(), (short) offset);
                            for(int i = 0; i < conditionList.size(); i++) {
                                isMatch = false;
                                columnIndex = conditionList.get(i).getIndex();
                                value = conditionList.get(i).getValue();
                                condition = conditionList.get(i).getConditionType();
                                if (record != null && record.getColumnValueList().size() > columnIndex) {
                                    Object object = record.getColumnValueList().get(columnIndex);
                                    if(((DT) object).isNull()) isMatch = false;
                                    else {
                                        switch (Utils.resolveClass(object)) {
                                            case Constants.TINYINT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TINYINT:
                                                        isMatch = ((DT_TinyInt) object).compare((DT_TinyInt) value, condition);
                                                        break;

                                                    case Constants.SMALLINT:
                                                        isMatch = ((DT_TinyInt) object).compare((DT_SmallInt) value, condition);
                                                        break;

                                                    case Constants.INT:
                                                        isMatch = ((DT_TinyInt) object).compare((DT_Int) value, condition);
                                                        break;

                                                    case Constants.BIGINT:
                                                        isMatch = ((DT_TinyInt) object).compare((DT_BigInt) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                                                }
                                                break;

                                            case Constants.SMALLINT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TINYINT:
                                                        isMatch = ((DT_SmallInt) object).compare((DT_TinyInt) value, condition);
                                                        break;

                                                    case Constants.SMALLINT:
                                                        isMatch = ((DT_SmallInt) object).compare((DT_SmallInt) value, condition);
                                                        break;

                                                    case Constants.INT:
                                                        isMatch = ((DT_SmallInt) object).compare((DT_Int) value, condition);
                                                        break;

                                                    case Constants.BIGINT:
                                                        isMatch = ((DT_SmallInt) object).compare((DT_BigInt) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                                                }
                                                break;

                                            case Constants.INT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TINYINT:
                                                        isMatch = ((DT_Int) object).compare((DT_TinyInt) value, condition);
                                                        break;

                                                    case Constants.SMALLINT:
                                                        isMatch = ((DT_Int) object).compare((DT_SmallInt) value, condition);
                                                        break;

                                                    case Constants.INT:
                                                        isMatch = ((DT_Int) object).compare((DT_Int) value, condition);
                                                        break;

                                                    case Constants.BIGINT:
                                                        isMatch = ((DT_Int) object).compare((DT_BigInt) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                                                }
                                                break;

                                            case Constants.BIGINT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TINYINT:
                                                        isMatch = ((DT_BigInt) object).compare((DT_TinyInt) value, condition);
                                                        break;

                                                    case Constants.SMALLINT:
                                                        isMatch = ((DT_BigInt) object).compare((DT_SmallInt) value, condition);
                                                        break;

                                                    case Constants.INT:
                                                        isMatch = ((DT_BigInt) object).compare((DT_Int) value, condition);
                                                        break;

                                                    case Constants.BIGINT:
                                                        isMatch = ((DT_BigInt) object).compare((DT_BigInt) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                                                }
                                                break;

                                            case Constants.REAL:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.REAL:
                                                        isMatch = ((DT_Real) object).compare((DT_Real) value, condition);
                                                        break;

                                                    case Constants.DOUBLE:
                                                        isMatch = ((DT_Real) object).compare((DT_Double) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Decimal Number");
                                                }
                                                break;

                                            case Constants.DOUBLE:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.REAL:
                                                        isMatch = ((DT_Double) object).compare((DT_Real) value, condition);
                                                        break;

                                                    case Constants.DOUBLE:
                                                        isMatch = ((DT_Double) object).compare((DT_Double) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Decimal Number");
                                                }
                                                break;

                                            case Constants.DATE:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.DATE:
                                                        isMatch = ((DT_Date) object).compare((DT_Date) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Date");
                                                }
                                                break;

                                            case Constants.DATETIME:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.DATETIME:
                                                        isMatch = ((DT_DateTime) object).compare((DT_DateTime) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Datetime");
                                                }
                                                break;

                                            case Constants.TEXT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TEXT:
                                                        if (((DT_Text) object).getValue() != null) {
                                                            if (condition != InternalCondition.EQUALS) {
                                                                throw new InternalException(InternalException.INVALID_CONDITION_EXCEPTION, "= is");
                                                            } else
                                                                isMatch = ((DT_Text) object).getValue().equalsIgnoreCase(((DT_Text) value).getValue());
                                                        }
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "String");
                                                }
                                                break;
                                        }
                                    }
                                    if(!isMatch) break;
                                }
                            }

                            if(isMatch) {
                                DataRecord matchedRecord = record;
                                if(selectionColumnIndexList != null) {
                                    matchedRecord = new DataRecord();
                                    matchedRecord.setRowId(record.getRowId());
                                    matchedRecord.setPageLocated(record.getPageLocated());
                                    matchedRecord.setOffset(record.getOffset());
                                    for (Byte index : selectionColumnIndexList) {
                                        matchedRecord.getColumnValueList().add(record.getColumnValueList().get(index));
                                    }
                                }
                                matchRecords.add(matchedRecord);
                                if(getOne) {
                                    randomAccessFile.close();
                                    return matchRecords;
                                }
                            }
                        }
                        if (page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return matchRecords;
                }
            } else {
                Utils.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
                return null;
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return null;
    }

    public int updateRecord(String databaseName, String tableName, List<Byte> searchColumnIndexList, List<Object> searchValueList, List<Short> searchConditionList, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        for (byte i = 0; i < searchColumnIndexList.size(); i++) {
            conditions.add(InternalCondition.CreateCondition(searchColumnIndexList.get(i), searchConditionList.get(i), searchValueList.get(i)));
        }
        return updateRecord(databaseName, tableName, conditions, updateColumnIndexList, updateColumnValueList, isIncrement);
    }

    public int updateRecord(String databaseName, String tableName, List<InternalCondition> conditions, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) throws InternalException {
        int updateRecordCount = 0;
        try {
            if (conditions == null || updateColumnIndexList == null
                    || updateColumnValueList == null)
                return updateRecordCount;
            if (updateColumnIndexList.size() != updateColumnValueList.size())
                return updateRecordCount;
            File file = new File(Utils.getDatabasePath(databaseName) + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                List<DataRecord> records = findRecord(databaseName, tableName, conditions, false);
                if (records != null) {
                    if (records.size() > 0) {
                        byte index;
                        Object object;
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                        for (DataRecord record : records) {
                            for (int i = 0; i < updateColumnIndexList.size(); i++) {
                                index = updateColumnIndexList.get(i);
                                object = updateColumnValueList.get(i);
                                if (isIncrement) {
                                    record.getColumnValueList().set(index, increment((DT_Numeric) record.getColumnValueList().get(index), (DT_Numeric) object));
                                } else {
                                    record.getColumnValueList().set(index, object);
                                }
                            }
                            this.writeRecord(randomAccessFile, record);
                            updateRecordCount++;
                        }
                        randomAccessFile.close();
                        return updateRecordCount;
                    }
                }
            } else {
                Utils.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return updateRecordCount;
    }

    private <T> DT_Numeric<T> increment(DT_Numeric<T> object1, DT_Numeric<T> object2) {
        object1.increment(object2.getValue());
        return object1;
    }

    public Page<DataRecord> getLastRecordAndPage(String databaseName, String tableName) throws InternalException {
        try {
            File file = new File(Utils.getDatabasePath(databaseName) + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                Page<DataRecord> page = getLastPage(file);
                if (page.getNumberOfCells() > 0) {
                    randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + Page.getHeaderFixedLength() + ((page.getNumberOfCells() - 1) * Short.BYTES));
                    short address = randomAccessFile.readShort();
                    DataRecord record = getDataRecord(randomAccessFile, page.getPageNumber(), address);
                    if (record != null)
                        page.getPageRecords().add(record);
                }
                randomAccessFile.close();
                return page;
            } else {
                Utils.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
                return null;
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private Page getLastPage(File file) throws InternalException {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE && page.getRightNodeAddress() != Page.RIGHTMOST_PAGE) {
                page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
            }
            randomAccessFile.close();
            return page;
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    private Page getFirstPage(File file) throws InternalException {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                if (page.getNumberOfCells() == 0) return null;
                randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + ((short) page.getRecordAddressList().get(0)));
                page = readPageHeader(randomAccessFile, randomAccessFile.readInt());
            }
            randomAccessFile.close();
            return page;
        } catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
    }

    public int deleteRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, boolean deleteOne) throws InternalException {
        int deletedRecordCount = 0;
        try {
            File file = new File(Utils.getDatabasePath(databaseName) + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                if(columnIndexList != null) {
                    Page page = getFirstPage(file);
                    DataRecord record;
                    boolean isMatch;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Short offset : new ArrayList<Short>(page.getRecordAddressList())) {
                            isMatch = true;
                            record = getDataRecord(randomAccessFile, page.getPageNumber(), offset);
                            for(int i = 0; i < columnIndexList.size(); i++) {
                                isMatch = false;
                                columnIndex = columnIndexList.get(i);
                                value = valueList.get(i);
                                condition = conditionList.get(i);
                                if (record != null && record.getColumnValueList().size() > columnIndex) {
                                    Object object = record.getColumnValueList().get(columnIndex);
                                    if(((DT) object).isNull()) isMatch = false;
                                    else {
                                        switch (Utils.resolveClass(object)) {
                                            case Constants.TINYINT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TINYINT:
                                                        isMatch = ((DT_TinyInt) object).compare((DT_TinyInt) value, condition);
                                                        break;

                                                    case Constants.SMALLINT:
                                                        isMatch = ((DT_TinyInt) object).compare((DT_SmallInt) value, condition);
                                                        break;

                                                    case Constants.INT:
                                                        isMatch = ((DT_TinyInt) object).compare((DT_Int) value, condition);
                                                        break;

                                                    case Constants.BIGINT:
                                                        isMatch = ((DT_TinyInt) object).compare((DT_BigInt) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                                                }
                                                break;

                                            case Constants.SMALLINT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TINYINT:
                                                        isMatch = ((DT_SmallInt) object).compare((DT_TinyInt) value, condition);
                                                        break;

                                                    case Constants.SMALLINT:
                                                        isMatch = ((DT_SmallInt) object).compare((DT_SmallInt) value, condition);
                                                        break;

                                                    case Constants.INT:
                                                        isMatch = ((DT_SmallInt) object).compare((DT_Int) value, condition);
                                                        break;

                                                    case Constants.BIGINT:
                                                        isMatch = ((DT_SmallInt) object).compare((DT_BigInt) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                                                }
                                                break;

                                            case Constants.INT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TINYINT:
                                                        isMatch = ((DT_Int) object).compare((DT_TinyInt) value, condition);
                                                        break;

                                                    case Constants.SMALLINT:
                                                        isMatch = ((DT_Int) object).compare((DT_SmallInt) value, condition);
                                                        break;

                                                    case Constants.INT:
                                                        isMatch = ((DT_Int) object).compare((DT_Int) value, condition);
                                                        break;

                                                    case Constants.BIGINT:
                                                        isMatch = ((DT_Int) object).compare((DT_BigInt) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                                                }
                                                break;

                                            case Constants.BIGINT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TINYINT:
                                                        isMatch = ((DT_BigInt) object).compare((DT_TinyInt) value, condition);
                                                        break;

                                                    case Constants.SMALLINT:
                                                        isMatch = ((DT_BigInt) object).compare((DT_SmallInt) value, condition);
                                                        break;

                                                    case Constants.INT:
                                                        isMatch = ((DT_BigInt) object).compare((DT_Int) value, condition);
                                                        break;

                                                    case Constants.BIGINT:
                                                        isMatch = ((DT_BigInt) object).compare((DT_BigInt) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Number");
                                                }
                                                break;

                                            case Constants.REAL:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.REAL:
                                                        isMatch = ((DT_Real) object).compare((DT_Real) value, condition);
                                                        break;

                                                    case Constants.DOUBLE:
                                                        isMatch = ((DT_Real) object).compare((DT_Double) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Decimal Number");
                                                }
                                                break;

                                            case Constants.DOUBLE:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.REAL:
                                                        isMatch = ((DT_Double) object).compare((DT_Real) value, condition);
                                                        break;

                                                    case Constants.DOUBLE:
                                                        isMatch = ((DT_Double) object).compare((DT_Double) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Decimal Number");
                                                }
                                                break;

                                            case Constants.DATE:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.DATE:
                                                        isMatch = ((DT_Date) object).compare((DT_Date) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Date");
                                                }
                                                break;

                                            case Constants.DATETIME:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.DATETIME:
                                                        isMatch = ((DT_DateTime) object).compare((DT_DateTime) value, condition);
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "Datetime");
                                                }
                                                break;

                                            case Constants.TEXT:
                                                switch (Utils.resolveClass(value)) {
                                                    case Constants.TEXT:
                                                        if (((DT_Text) object).getValue() != null) {
                                                            if (condition != InternalCondition.EQUALS) {
                                                                throw new InternalException(InternalException.INVALID_CONDITION_EXCEPTION, "= is");
                                                            } else
                                                                isMatch = ((DT_Text) object).getValue().equalsIgnoreCase(((DT_Text) value).getValue());
                                                        }
                                                        break;

                                                    default:
                                                        throw new InternalException(InternalException.DATATYPE_MISMATCH_EXCEPTION, "String");
                                                }
                                                break;
                                        }
                                    }
                                    if(!isMatch) break;
                                }
                            }
                            if(isMatch) {
                                page.setNumberOfCells((byte) (page.getNumberOfCells() - 1));
                                page.getRecordAddressList().remove(offset);
                                if(page.getNumberOfCells() == 0) {
                                    page.setStartingAddress((short) (page.getBaseAddress() + Page.PAGE_SIZE - 1));
                                }
                                this.writePageHeader(randomAccessFile, page);
                                UpdateStatementHelper.decrementRowCount(databaseName, tableName);
                                deletedRecordCount++;
                                if(deleteOne) {
                                    randomAccessFile.close();
                                    return deletedRecordCount;
                                }
                            }
                        }
                        if(page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return deletedRecordCount;
                }
            }
            else {
                Utils.printMessage(String.format("Table '%s.%s' doesn't exist.", databaseName, tableName));
                return deletedRecordCount;
            }
        }
        catch (InternalException e) {
            throw e;
        }
        catch (ClassCastException e) {
            throw new InternalException(InternalException.INVALID_DATATYPE_EXCEPTION);
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return deletedRecordCount;
    }

    public DataRecord getDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address) throws InternalException {
        try {
            if (pageNumber >= 0 && address >= 0) {
                DataRecord record = new DataRecord();
                record.setPageLocated(pageNumber);
                record.setOffset(address);
                randomAccessFile.seek((Page.PAGE_SIZE * pageNumber) + address);
                record.setSize(randomAccessFile.readShort());
                record.setRowId(randomAccessFile.readInt());
                byte numberOfColumns = randomAccessFile.readByte();
                byte[] serialTypeCodes = new byte[numberOfColumns];
                for (byte i = 0; i < numberOfColumns; i++) {
                    serialTypeCodes[i] = randomAccessFile.readByte();
                }
                Object object;
                for (byte i = 0; i < numberOfColumns; i++) {
                    switch (serialTypeCodes[i]) {
                        //case DT_TinyInt.nullSerialCode is overridden with DT_Text

                        case Constants.ONE_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DT_Text(null);
                            break;

                        case Constants.TWO_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DT_SmallInt(randomAccessFile.readShort(), true);
                            break;

                        case Constants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DT_Real(randomAccessFile.readFloat(), true);
                            break;

                        case Constants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE:
                            object = new DT_Double(randomAccessFile.readDouble(), true);
                            break;

                        case Constants.TINY_INT_SERIAL_TYPE_CODE:
                            object = new DT_TinyInt(randomAccessFile.readByte());
                            break;

                        case Constants.SMALL_INT_SERIAL_TYPE_CODE:
                            object = new DT_SmallInt(randomAccessFile.readShort());
                            break;

                        case Constants.INT_SERIAL_TYPE_CODE:
                            object = new DT_Int(randomAccessFile.readInt());
                            break;

                        case Constants.BIG_INT_SERIAL_TYPE_CODE:
                            object = new DT_BigInt(randomAccessFile.readLong());
                            break;

                        case Constants.REAL_SERIAL_TYPE_CODE:
                            object = new DT_Real(randomAccessFile.readFloat());
                            break;

                        case Constants.DOUBLE_SERIAL_TYPE_CODE:
                            object = new DT_Double(randomAccessFile.readDouble());
                            break;

                        case Constants.DATE_SERIAL_TYPE_CODE:
                            object = new DT_Date(randomAccessFile.readLong());
                            break;

                        case Constants.DATE_TIME_SERIAL_TYPE_CODE:
                            object = new DT_DateTime(randomAccessFile.readLong());
                            break;

                        case Constants.TEXT_SERIAL_TYPE_CODE:
                            object = new DT_Text("");
                            break;

                        default:
                            if (serialTypeCodes[i] > Constants.TEXT_SERIAL_TYPE_CODE) {
                                byte length = (byte) (serialTypeCodes[i] - Constants.TEXT_SERIAL_TYPE_CODE);
                                char[] text = new char[length];
                                for (byte k = 0; k < length; k++) {
                                    text[k] = (char) randomAccessFile.readByte();
                                }
                                object = new DT_Text(new String(text));
                            } else
                                object = null;
                            break;
                    }
                    record.getColumnValueList().add(object);
                }
                return record;
            }
        } catch (ClassCastException e) {
            throw new InternalException(InternalException.INVALID_DATATYPE_EXCEPTION);
        }
        catch (Exception e) {
            throw new InternalException(InternalException.GENERIC_EXCEPTION);
        }
        return null;
    }


    // ====================================================================================
    // Query processing methods
    // ====================================================================================
    public List<String> fetchAllTableColumns(String databaseName, String tableName) throws InternalException {
        List<String> columnNames = new ArrayList<>();
        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition condition = new InternalCondition();
        condition.setIndex(CatalogDB.COLUMNS_TABLE_SCHEMA_DATABASE_NAME);
        condition.setValue(new DT_Text(databaseName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);
        condition = new InternalCondition();
        condition.setIndex(CatalogDB.COLUMNS_TABLE_SCHEMA_TABLE_NAME);
        condition.setValue(new DT_Text(tableName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);

        List<DataRecord> records = this.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnNames.add(((DT) object).getStringValue());
        }

        return columnNames;
    }

    public boolean checkNullConstraint(String databaseName, String tableName, HashMap<String, Integer> columnMap) throws InternalException {

        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition condition = new InternalCondition();
        condition.setIndex(CatalogDB.COLUMNS_TABLE_SCHEMA_DATABASE_NAME);
        condition.setValue(new DT_Text(databaseName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);
        condition = new InternalCondition();
        condition.setIndex(CatalogDB.COLUMNS_TABLE_SCHEMA_TABLE_NAME);
        condition.setValue(new DT_Text(tableName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);

        List<DataRecord> records = this.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object nullValueObject = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_IS_NULLABLE);
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);

            String isNullStr = ((DT) nullValueObject).getStringValue();
            boolean isNullable = (isNullStr.compareToIgnoreCase("NULL") == 0) ? false : true;
            if (isNullable) {
                isNullable = (isNullStr.compareToIgnoreCase("NO") == 0) ? true : false;
            }

            if (!columnMap.containsKey(((DT) object).getStringValue()) && isNullable) {
                Utils.printMessage("Field '" + ((DT) object).getStringValue() + "' doesn't have a default value");
                return false;
            }

        }

        return true;
    }

    public HashMap<String, Integer> fetchAllTableColumnDataTypes(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition condition = new InternalCondition();
        condition.setIndex(CatalogDB.COLUMNS_TABLE_SCHEMA_DATABASE_NAME);
        condition.setValue(new DT_Text(databaseName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);
        condition = new InternalCondition();
        condition.setIndex(CatalogDB.COLUMNS_TABLE_SCHEMA_TABLE_NAME);
        condition.setValue(new DT_Text(tableName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);

        List<DataRecord> records = this.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);
        HashMap<String, Integer> columDataTypeMapping = new HashMap<>();

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            Object dataTypeObject = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_DATA_TYPE);

            String columnName = ((DT) object).getStringValue();
            int columnDataType = Utils.stringToDataType(((DT) dataTypeObject).getStringValue());
            columDataTypeMapping.put(columnName.toLowerCase(), columnDataType);
        }

        return columDataTypeMapping;
    }

    public String getTablePrimaryKey(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();

        DT_Text tableNameObj = new DT_Text(tableName);
        DT_Text primaryKeyObj = new DT_Text(CatalogDB.PRIMARY_KEY_IDENTIFIER);
        DT_Text databaseObj = new DT_Text(databaseName);


        conditions.add(InternalCondition.CreateCondition(CatalogDB.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, databaseObj));
        conditions.add(InternalCondition.CreateCondition(CatalogDB.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, tableNameObj));
        conditions.add(InternalCondition.CreateCondition(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_KEY, InternalCondition.EQUALS, primaryKeyObj));

        List<DataRecord> records = this.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, conditions, false);
        String columnName = "";
        for (DataRecord record : records) {
            Object object = record.getColumnValueList().get(CatalogDB.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnName = ((DT) object).getStringValue();
            break;
        }

        return columnName;
    }

    public int getTableRecordCount(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition condition = new InternalCondition();
        condition.setIndex(CatalogDB.TABLES_TABLE_SCHEMA_DATABASE_NAME);
        condition.setValue(new DT_Text(databaseName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);
        condition = new InternalCondition();
        condition.setIndex(CatalogDB.TABLES_TABLE_SCHEMA_TABLE_NAME);
        condition.setValue(new DT_Text(tableName));
        condition.setConditionType(InternalCondition.EQUALS);
        conditions.add(condition);

        List<DataRecord> records = this.findRecord(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, conditions, true);
        int recordCount = 0;

        for (DataRecord record : records) {
            Object object = record.getColumnValueList().get(CatalogDB.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            recordCount = Integer.valueOf(((DT) object).getStringValue());
            break;
        }

        return recordCount;
    }

    public boolean checkIfValueForPrimaryKeyExists(String databaseName, String tableName, int value) throws InternalException {
        StorageManager manager = new StorageManager();
        InternalCondition condition = InternalCondition.CreateCondition(0, InternalCondition.EQUALS, new DT_Int(value));

        List<DataRecord> records = manager.findRecord(databaseName, tableName, condition, false);
        if (records.size() > 0) {
            return true;
        }
        else {
            return false;
        }
    }
}
