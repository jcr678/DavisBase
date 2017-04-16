package storage;

import Model.Column;
import Model.Record;
import common.Constants;
import common.Utils;
import console.ConsoleWriter;
import datatypes.*;
import datatypes.base.DT;
import datatypes.base.DT_Numeric;
import storage.model.DataRecord;
import storage.model.Page;
import storage.model.PointerRecord;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dakle on 9/4/17.
 */
public class StorageManager {

    private String DEFAULT_DATA_PATH = Constants.DEFAULT_DATA_DIRNAME;

    public boolean createDatabase(String databaseName) {
        try {
            File dirFile = new File(DEFAULT_DATA_PATH + "/" + databaseName);
            if(dirFile.exists()) {
                System.out.println("Database " + databaseName + " already exists!");
                return false;
            }
            return dirFile.mkdirs();
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /***
     * Checks if the database with name 'databaseName' exists in the system.
     * @param databaseName
     * @return True if the database exists else false.
     */
    public static boolean checkDatabaseExists(String databaseName) {
        File dirFile = new File(databaseName);
        if(!dirFile.exists()) {
            return false;
        }

        return true;
    }

    public boolean createTable(String databaseName, String tableName) {
        try {
            File dirFile = new File(databaseName);
            if(!dirFile.exists()) {
                dirFile.mkdir();
            }
            File file = new File(databaseName + "/" + tableName);
            if(file.exists()) {
                return false;
            }
            if(file.createNewFile()) {
                RandomAccessFile randomAccessFile;
                Page<DataRecord> page = Page.createNewEmptyPage(new DataRecord());
                randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(Page.PAGE_SIZE);
                return writePageHeader(randomAccessFile, page);
            }
            return false;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /***
     * Checks if the table exists in the database.
     * @param databaseName
     * @param tableName
     * @return True if the table exists else False.
     */
    public static boolean checkTableExists(String databaseName, String tableName) {
        File dirFile = new File(databaseName);
        if(!dirFile.exists()) {
            return false;
        }

        File file = new File( databaseName + "/" + tableName);
        if(!file.exists()) {
            return false;
        }

        return true;
    }

    public static boolean defaultDatabaseExists() {
        if (StorageManager.checkDatabaseExists(Utils.getUserDatabasePath(Constants.DEFAULT_USER_DATABASE))) {
            return true;
        }
        else {
            return false;
        }
    }

    public static boolean tableExistsInDefaultDatabase (String tableName) {
        StorageManager storageManager = new StorageManager();
        if (storageManager.checkTableExists(Constants.DEFAULT_USER_DATABASE, tableName)) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean writeRecord(String databaseName, String tableName, DataRecord record) {
        try {
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                Page page = getPage(randomAccessFile, record, 0);
                if(page == null) return false;
                if(!checkSpaceRequirements(page, record)) {
                    int pageCount = (int) (randomAccessFile.length() / Page.PAGE_SIZE);
                    switch (pageCount) {
                        case 1:
                            PointerRecord pointerRecord = splitPage(randomAccessFile, page, record, 1, 2);
                            Page<PointerRecord> pointerRecordPage = Page.createNewEmptyPage(pointerRecord);
                            pointerRecordPage.setPageNumber(0);
                            pointerRecordPage.setPageType(Page.INTERIOR_TABLE_PAGE);
                            pointerRecordPage.setNumberOfCells((byte) 1);
                            pointerRecordPage.setStartingAddress((short)(pointerRecordPage.getStartingAddress() - pointerRecord.getSize()));
                            pointerRecordPage.setRightNodeAddress(2);
                            pointerRecordPage.getRecordAddressList().add((short) (pointerRecordPage.getStartingAddress() + 1));
                            pointerRecord.setOffset((short) (pointerRecordPage.getStartingAddress() + 1));
                            this.writePageHeader(randomAccessFile, pointerRecordPage);
                            this.writeRecord(randomAccessFile, pointerRecord);
                            break;

                        default:
                            if(pageCount > 1) {
                                System.out.println("Well things look pretty darn bad");
                            }
                            break;
                    }
                    randomAccessFile.close();
                    return true;
                }
                short address = (short) getAddress(file, record.getRowId(), page.getPageNumber());
//                System.out.println(address);
                page.setNumberOfCells((byte)(page.getNumberOfCells() + 1));
                page.setStartingAddress((short) (page.getStartingAddress() - record.getSize() - record.getHeaderSize()));
                page.getRecordAddressList().add((short)(page.getStartingAddress() + 1));
                record.setPageLocated(page.getPageNumber());
                record.setOffset((short)(page.getStartingAddress() + 1));
                this.writePageHeader(randomAccessFile, page);
                this.writeRecord(randomAccessFile, record);
                randomAccessFile.close();
            } else {
                ConsoleWriter.displayMessage("File " + tableName + " does not exist");
            }
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean checkSpaceRequirements(Page page, DataRecord record) {
        if(page != null && record != null) {
            short endingAddress = page.getStartingAddress();
            short startingAddress = (short) (Page.getHeaderFixedLength() + (page.getRecordAddressList().size() * Short.BYTES));
            return (record.getSize() + record.getHeaderSize() + Short.BYTES) <= (endingAddress - startingAddress);
        }
        return false;
    }

    private PointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, DataRecord record, int pageNumber1, int pageNumber2) {
        try {
            if (page != null && record != null) {
                int location = -1;
                PointerRecord pointerRecord = new PointerRecord();
                if(page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType());
                randomAccessFile.setLength(Page.PAGE_SIZE * (pageNumber2 + 1));
                if(location == page.getNumberOfCells()) {
                    Page<DataRecord> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setNumberOfCells(page.getNumberOfCells());
                    page1.setRightNodeAddress(pageNumber2);
                    page1.setStartingAddress(page.getStartingAddress());
                    page1.setRecordAddressList(page.getRecordAddressList());
                    this.writePageHeader(randomAccessFile, page1);
                    List<DataRecord> records = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, page.getNumberOfCells(), page1.getPageNumber(), record);
                    for(DataRecord object: records) {
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
                }
                else {
                    //Handle this when a record is being inserted in middle
                }
                pointerRecord.setLeftPageNumber(pageNumber1);
                pointerRecord.setKey(record.getRowId());
                return pointerRecord;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private PointerRecord splitPage(RandomAccessFile randomAccessFile, Page page, PointerRecord record, int pageNumber1, int pageNumber2) {
        try {
            if (page != null && record != null) {
                int location = -1;
                PointerRecord pointerRecord = new PointerRecord();
                if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                    return null;
                }
                location = binarySearch(randomAccessFile, record.getKey(), page.getNumberOfCells(), ((page.getPageNumber() * Page.PAGE_SIZE) + Page.getHeaderFixedLength()), page.getPageType());
                if(location == page.getNumberOfCells()) {
                    Page<PointerRecord> page1 = new Page<>(pageNumber1);
                    page1.setPageType(page.getPageType());
                    page1.setNumberOfCells(page.getNumberOfCells());
                    page1.setRightNodeAddress(pageNumber2);
//                    for(Object offset : page.getRecordAddressList()) {
//                        page1.getRecordAddressList().add((short) offset);
//                    }
                    page1.setRecordAddressList(page.getRecordAddressList());
                    page1.setPageNumber(pageNumber1);
                    randomAccessFile.seek(Page.PAGE_SIZE * page1.getPageNumber());
                    this.writePageHeader(randomAccessFile, page1);
                    List<PointerRecord> records = copyRecords(randomAccessFile, (page.getPageNumber() * Page.PAGE_SIZE), page.getRecordAddressList(), (byte) 0, page.getNumberOfCells(), page1.getPageNumber(), record);
                    for(PointerRecord object: records) {
                        this.writeRecord(randomAccessFile, object);
                    }
                    Page<PointerRecord> page2 = new Page<>(pageNumber2);
                    page2.setPageType(page.getPageType());
                    page2.setNumberOfCells((byte) 1);
                    page2.setRightNodeAddress(page.getRightNodeAddress());
                    page2.setStartingAddress((short) (page2.getStartingAddress() - record.getSize()));
                    page2.getRecordAddressList().add((short) (page2.getStartingAddress() - page2.getBaseAddress() + 1));
                    page2.setPageNumber(pageNumber2);
                    randomAccessFile.seek(page2.getBaseAddress());
                    this.writePageHeader(randomAccessFile, page2);
                    this.writeRecord(randomAccessFile, record);
                }
                else {
                    //Handle this when a record is being inserted in middle
                }
                pointerRecord.setLeftPageNumber(pageNumber1);
                pointerRecord.setKey(record.getKey());
                return pointerRecord;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private <T> List<T> copyRecords(RandomAccessFile randomAccessFile, long pageStartAddress, List<Short> recordAddresses, byte startIndex, byte endIndex, int pageNumber, T object) {
        try {
            List<T> records = new ArrayList<>();
            byte numberOfRecords;
            byte[] serialTypeCodes;
            for (byte i = startIndex; i < endIndex; i++) {
                randomAccessFile.seek(pageStartAddress + recordAddresses.get(i));
                if(object.getClass().equals(PointerRecord.class)) {
                    PointerRecord record = new PointerRecord();
                    record.setLeftPageNumber(randomAccessFile.readInt());
                    record.setKey(randomAccessFile.readInt());
                    records.add((T) record);
                }
                else if(object.getClass().equals(DataRecord.class)) {
                    DataRecord record = new DataRecord();
                    record.setPageLocated(pageNumber);
                    record.setOffset(recordAddresses.get(i));
                    record.setSize(randomAccessFile.readShort());
                    record.setRowId(randomAccessFile.readInt());
                    numberOfRecords = randomAccessFile.readByte();
                    serialTypeCodes = new byte[numberOfRecords];
                    for(byte j = 0; j < numberOfRecords; j++) {
                        serialTypeCodes[j] = randomAccessFile.readByte();
                    }
                    for(byte j = 0; j < numberOfRecords; j++) {
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
                    records.add((T) record);
                }
            }
            return records;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Page getPage(RandomAccessFile randomAccessFile, DataRecord record, int pageNumber) {
        try {
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
               return page;
            }
            pageNumber = binarySearch(randomAccessFile, record.getRowId(), page.getNumberOfCells(), (page.getBaseAddress() + Page.getHeaderFixedLength()), Page.INTERIOR_TABLE_PAGE);
            if(pageNumber == -1) return null;
            return getPage(randomAccessFile, record, pageNumber);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private int getAddress(File file, int rowId, int pageNumber) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                return binarySearch(randomAccessFile, rowId, page.getNumberOfCells(), randomAccessFile.getFilePointer(), Page.LEAF_TABLE_PAGE);
            }
            return -1;
        }
        catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    private int binarySearch(RandomAccessFile randomAccessFile, int key, int numberOfRecords, long seekPosition, byte pageType) {
        try {
            int start = 0, end = numberOfRecords;
            int mid;
            int pageNumber = -1;
            int rowId;
            short address;
            while(true) {
                if(start > end) {
                    if(pageType == Page.LEAF_TABLE_PAGE)
                        return start;
                    if(pageType == Page.INTERIOR_TABLE_PAGE) {
                        if(end < 0)
                            return pageNumber;
                        randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + 4);
                        return randomAccessFile.readInt();
                    }
                }
                mid = (start + end) / 2;
                randomAccessFile.seek(seekPosition + (Short.BYTES * mid));
                address = randomAccessFile.readShort();
                randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + address);
                if(pageType == Page.LEAF_TABLE_PAGE) {
                    randomAccessFile.readShort();
                    rowId = randomAccessFile.readInt();
                    if(rowId == key)    return mid;
                    if(rowId > key) {
                        end = mid - 1;
                    }
                    else {
                        start = mid + 1;
                    }
                }
                else if(pageType == Page.INTERIOR_TABLE_PAGE) {
                    pageNumber = randomAccessFile.readInt();
                    rowId = randomAccessFile.readInt();
                    if(rowId > key) {
                        end = mid - 1;
                    }
                    else {
                        start = mid + 1;
                    }
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    private Page readPageHeader(RandomAccessFile randomAccessFile, int pageNumber) {
        try {
            Page page;
            randomAccessFile.seek(Page.PAGE_SIZE * pageNumber);
            byte pageType = randomAccessFile.readByte();
            if(pageType == Page.INTERIOR_TABLE_PAGE) {
                page = new Page<PointerRecord>();
            }
            else {
                page = new Page<DataRecord>();
            }
            page.setPageType(pageType);
            page.setPageNumber(pageNumber);
            page.setNumberOfCells(randomAccessFile.readByte());
            page.setStartingAddress(randomAccessFile.readShort());
            page.setRightNodeAddress(randomAccessFile.readInt());
            for(byte i = 0; i < page.getNumberOfCells(); i++) {
                page.getRecordAddressList().add(randomAccessFile.readShort());
            }
            return page;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private boolean writePageHeader(RandomAccessFile randomAccessFile, Page page) {
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
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, DataRecord record) {
        try {
            randomAccessFile.seek((record.getPageLocated() * Page.PAGE_SIZE) + record.getOffset());
            randomAccessFile.writeShort(record.getSize());
            randomAccessFile.writeInt(record.getRowId());
            randomAccessFile.writeByte((byte) record.getColumnValueList().size());
            randomAccessFile.write(record.getSerialTypeCodes());
            for (Object object: record.getColumnValueList()) {
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
                        if(((DT_Text) object).getValue() != null)
                            randomAccessFile.writeBytes(((DT_Text) object).getValue());
                        break;

                    default:
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean writeRecord(RandomAccessFile randomAccessFile, PointerRecord record) {
        try {
            randomAccessFile.seek((record.getPageNumber() * Page.PAGE_SIZE) + record.getOffset());
            randomAccessFile.writeInt(record.getLeftPageNumber());
            randomAccessFile.writeInt(record.getKey());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, boolean getOne) {
        return findRecord(databaseName, tableName, columnIndexList, valueList, conditionList, null, getOne);
    }

    public List<DataRecord> findRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, List<Short> conditionList, List<Byte> selectionColumnIndexList, boolean getOne) {
        try {
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                if(columnIndexList.size() > 0) {
                    Page page = getFirstPage(file);
                    DataRecord record;
                    List<DataRecord> matchRecords = new ArrayList<>();
                    boolean isMatch = false;
                    byte columnIndex;
                    short condition;
                    Object value;
                    while (page != null) {
                        for (Object offset : page.getRecordAddressList()) {
                            record = getDataRecord(randomAccessFile, page.getPageNumber(), (short) offset, selectionColumnIndexList);
                            for(int i = 0; i < columnIndexList.size(); i++) {
                                columnIndex = columnIndexList.get(i);
                                value = valueList.get(i);
                                condition = conditionList.get(i);
                                if (record != null && record.getColumnValueList().size() > columnIndex) {
                                    Object object = record.getColumnValueList().get(columnIndex);
                                    switch (Utils.resolveClass(value)) {
                                        case Constants.TINYINT:
                                            isMatch = ((DT_TinyInt) value).compare((DT_TinyInt) object, condition);
                                            break;

                                        case Constants.SMALLINT:
                                            isMatch = ((DT_SmallInt) value).compare((DT_SmallInt) object, condition);
                                            break;

                                        case Constants.INT:
                                            isMatch = ((DT_Int) value).compare((DT_Int) object, condition);
                                            break;

                                        case Constants.BIGINT:
                                            isMatch = ((DT_BigInt) value).compare((DT_BigInt) object, condition);
                                            break;

                                        case Constants.REAL:
                                            isMatch = ((DT_Real) value).compare((DT_Real) object, condition);
                                            break;

                                        case Constants.DOUBLE:
                                            isMatch = ((DT_Double) value).compare((DT_Double) object, condition);
                                            break;

                                        case Constants.DATE:
                                            isMatch = ((DT_Date) value).compare((DT_Date) object, condition);
                                            break;

                                        case Constants.DATETIME:
                                            isMatch = ((DT_DateTime) value).compare((DT_DateTime) object, condition);
                                            break;

                                        case Constants.TEXT:
                                            isMatch = ((DT_Text) value).getValue().equalsIgnoreCase(((DT_Text) object).getValue());
                                            break;
                                    }
                                }
                            }
                            if(isMatch) {
                                matchRecords.add(record);
                                if(getOne) {
                                    randomAccessFile.close();
                                    return matchRecords;
                                }
                            }
                        }
                        if(page.getRightNodeAddress() == Page.RIGHTMOST_PAGE)
                            break;
                        page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
                    }
                    randomAccessFile.close();
                    return matchRecords;
                }
            }
            else {
                ConsoleWriter.displayMessage("Table " + tableName + " does not exist");
                return null;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean updateRecord(String databaseName, String tableName, List<Byte> searchColumnsIndexList, List<Object> searchKeysValueList, List<Short> searchKeysConditionsList, List<Byte> updateColumnIndexList, List<Object> updateColumnValueList, boolean isIncrement) {
        try {
            if(searchColumnsIndexList == null || searchKeysValueList == null
                    || searchKeysConditionsList == null || updateColumnIndexList == null
                    || updateColumnValueList == null)
                return false;
            if(searchColumnsIndexList.size() != searchKeysValueList.size() && searchKeysValueList.size() != searchKeysConditionsList.size())
                return false;
            if(updateColumnIndexList.size() != updateColumnValueList.size())
                return false;
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if(file.exists()) {
                List<DataRecord> records = findRecord(databaseName, tableName, searchColumnsIndexList, searchKeysValueList, searchKeysConditionsList, false);
                if (records != null) {
                    if (records.size() > 0) {
                        byte index;
                        Object object;
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                        for (DataRecord record : records) {
                            for (int i = 0; i < updateColumnIndexList.size(); i++) {
                                index = updateColumnIndexList.get(i);
                                object = updateColumnValueList.get(i);
                                if(isIncrement) {
                                    record.getColumnValueList().set(index, increment((DT_Numeric) record.getColumnValueList().get(index), (DT_Numeric) object));
                                }
                                else {
                                    record.getColumnValueList().set(index, object);
                                }
                            }
                            this.writeRecord(randomAccessFile, record);
                        }
                        randomAccessFile.close();
                        return true;
                    }
                }
            }
            else {
                ConsoleWriter.displayMessage("Table " + tableName + " does not exist!");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private <T> DT_Numeric<T> increment(DT_Numeric<T> object1, DT_Numeric<T> object2) {
        object1.increment(object2.getValue());
        return object1;
    }

    public Page<DataRecord> getLastRecordAndPage(String databaseName, String tableName) {
        try {
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                Page<DataRecord> page = getLastPage(file);
                if(page.getNumberOfCells() > 0) {
                    randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + Page.getHeaderFixedLength() + ((page.getNumberOfCells() - 1) * Short.BYTES));
                    short address = randomAccessFile.readShort();
                    DataRecord record = getDataRecord(randomAccessFile, page.getPageNumber(), address);
                    if(record != null)
                        page.getPageRecords().add(record);
                }
                return page;

            } else {
                ConsoleWriter.displayMessage("File " + tableName + " does not exist");
                return null;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Page getLastPage(File file) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE && page.getRightNodeAddress() != Page.RIGHTMOST_PAGE) {
                page = readPageHeader(randomAccessFile, page.getRightNodeAddress());
            }
            return page;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Page getFirstPage(File file) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                if(page.getNumberOfCells() == 0) return null;
                randomAccessFile.seek((Page.PAGE_SIZE * page.getPageNumber()) + ((short) page.getRecordAddressList().get(0)));
                page = readPageHeader(randomAccessFile, randomAccessFile.readInt());
            }
            randomAccessFile.close();
            return page;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public DataRecord getDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address) {
        return getDataRecord(randomAccessFile, pageNumber, address, null);
    }

    public DataRecord getDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address, List<Byte> columnList) {
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
                            }
                            else
                                object = null;
                            break;
                    }
                    if(columnList != null && !columnList.contains(i))    continue;
                    record.getColumnValueList().add(object);
                }
                return record;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
