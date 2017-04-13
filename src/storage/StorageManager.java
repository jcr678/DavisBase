package storage;

import common.Constants;
import common.Utils;
import console.ConsoleWriter;
import datatypes.*;
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

    public static String DEFAULT_DATA_PATH = Constants.DEFAULT_DATA_DIRNAME;

    public boolean createDatabase(String databaseName) {
        try {
            File dirFile = new File(DEFAULT_DATA_PATH + "/" + databaseName);
            if(dirFile.exists()) {
                System.out.println("Database " + databaseName + " already exists!");
                return false;
            }
            return dirFile.mkdir();
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
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
                Page page = Page.createNewEmptyPage();
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

    public boolean writeRecord(String databaseName, String tableName, DataRecord record) {
        try {
            File file = new File(databaseName + "/" + tableName + Constants.DEFAULT_FILE_EXTENSION);
            if (file.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                Page page = getPage(file, record.getRowId());
                if(page == null) return false;
                if(!checkSpaceRequirements(page, record)) {
                    System.out.println("Page Over Flow!");
                    return false;
                }
                short address = (short) getAddress(file, record.getRowId(), page.getPageNumber());
                page.setNumberOfCells((byte)(page.getNumberOfCells() + 1));
                page.setStartingAddress((short) (page.getStartingAddress() - record.getSize() - record.getHeaderSize()));
                page.getRecordAddressList().add((short)(page.getStartingAddress() + 1));
                this.writePageHeader(randomAccessFile, page);
                this.writeRecord(randomAccessFile, record, (short)(page.getStartingAddress() + 1));
                randomAccessFile.close();
                /*
                 * Check if page has space.
                 * if yes:
                 *      write the record, modify the header
                 *    no:
                 *      split the page
                 * Update data in system tables.
                 */
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

    private Page getPage(File file, int rowId) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, 0);
            int pageNumber;
            while (page.getPageType() == Page.INTERIOR_TABLE_PAGE) {
                pageNumber = binarySearch(randomAccessFile, rowId, page.getNumberOfCells(), randomAccessFile.getFilePointer(), Page.INTERIOR_TABLE_PAGE);
                if(pageNumber == -1) return null;
                page = readPageHeader(randomAccessFile, pageNumber);
            }
            return page;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private int getAddress(File file, int rowId, int pageNumber) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            Page page = readPageHeader(randomAccessFile, pageNumber);
            if(page.getPageType() == Page.LEAF_TABLE_PAGE) {
                return binarySearch(randomAccessFile, rowId, page.getNumberOfCells(), randomAccessFile.getFilePointer(), Page.INTERIOR_TABLE_PAGE);
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
            randomAccessFile.seek(seekPosition);
            int start = 0, end = numberOfRecords;
            int mid;
            int pageNumber;
            int rowId;
            short address;
            while(true) {
                if(start > end || start == numberOfRecords) return -1;
                mid = (start + end) / 2;
                randomAccessFile.seek(Short.BYTES * mid);
                address = randomAccessFile.readShort();
                randomAccessFile.seek(seekPosition - Page.getHeaderFixedLength() + address);
                if(pageType == Page.LEAF_TABLE_PAGE) {
                    randomAccessFile.readShort();
                    rowId = randomAccessFile.readInt();
                    if(rowId == key)    return address;
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
                    if(rowId == key) {
                        return pageNumber;
                    }
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
            randomAccessFile.seek(0);
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

    private boolean writeRecord(RandomAccessFile randomAccessFile, DataRecord record, short position) {
        try {
            randomAccessFile.seek(position);
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

    public List<DataRecord> findRecord(String databaseName, String tableName, List<Byte> columnIndexList, List<Object> valueList, boolean getOne) {
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
                    Object value;
                    while (page != null) {
                        for (Object offset : page.getRecordAddressList()) {
                            record = getDataRecord(randomAccessFile, page.getPageNumber(), (short) offset);
                            for(int i = 0; i < columnIndexList.size(); i++) {
                                columnIndex = columnIndexList.get(i);
                                value = valueList.get(i);
                                if (record != null && record.getColumnValueList().size() > columnIndex) {
                                    Object object = record.getColumnValueList().get(columnIndex);
                                    switch (Utils.resolveClass(value)) {
                                        case Constants.TINYINT:
                                            isMatch = ((DT_TinyInt) value).getValue() == ((DT_TinyInt) object).getValue();
                                            break;

                                        case Constants.SMALLINT:
                                            isMatch = ((DT_SmallInt) value).getValue() == ((DT_SmallInt) object).getValue();
                                            break;

                                        case Constants.INT:
                                            isMatch = ((DT_Int) value).getValue() == ((DT_Int) object).getValue();
                                            break;

                                        case Constants.BIGINT:
                                            isMatch = ((DT_BigInt) value).getValue() == ((DT_BigInt) object).getValue();
                                            break;

                                        case Constants.REAL:
                                            isMatch = ((DT_Real) value).getValue() == ((DT_Real) object).getValue();
                                            break;

                                        case Constants.DOUBLE:
                                            isMatch = ((DT_Double) value).getValue() == ((DT_Double) object).getValue();
                                            break;

                                        case Constants.DATE:
                                            isMatch = ((DT_Date) value).getValue() == ((DT_Date) object).getValue();
                                            break;

                                        case Constants.DATETIME:
                                            isMatch = ((DT_DateTime) value).getValue() == ((DT_DateTime) object).getValue();
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
                ConsoleWriter.displayMessage("File " + tableName + " does not exist");
                return null;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean updateRecord(String databaseName, String tableName, int searchColumnIndex, Object searchKey, List<Integer> updateColumnIndexList, List<Object> updateColumnValueList) {
        return true;
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
            return page;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public DataRecord getDataRecord(RandomAccessFile randomAccessFile, int pageNumber, short address) {
        try {
            if (pageNumber >= 0 && address >= 0 && address <= Page.PAGE_SIZE) {
                DataRecord record = new DataRecord();
                randomAccessFile.seek((Page.PAGE_SIZE * pageNumber) + address);
                record.setSize(randomAccessFile.readShort());
                record.setRowId(randomAccessFile.readInt());
                byte numberOfColumns = randomAccessFile.readByte();
                byte[] serialTypeCodes = new byte[numberOfColumns];
                for (byte i = 0; i < numberOfColumns; i++) {
                    serialTypeCodes[i] = randomAccessFile.readByte();
                }
                for (byte i = 0; i < numberOfColumns; i++) {
                    switch (serialTypeCodes[i]) {
                        //case DT_TinyInt.nullSerialCode is overridden with DT_Text

                        case DT_TinyInt.valueSerialCode:
                            record.getColumnValueList().add(new DT_TinyInt(randomAccessFile.readByte()));
                            break;

                        case DT_SmallInt.nullSerialCode:
                            record.getColumnValueList().add(new DT_SmallInt(randomAccessFile.readShort(), true));
                            break;

                        case DT_SmallInt.valueSerialCode:
                            record.getColumnValueList().add(new DT_SmallInt(randomAccessFile.readShort()));
                            break;

                        //case DT_Int.nullSerialCode is overridden with DT_Real

                        case DT_Int.valueSerialCode:
                            record.getColumnValueList().add(new DT_Int(randomAccessFile.readInt()));
                            break;

                        //case DT_BigInt.nullSerialCode is overridden with DT_Double

                        case DT_BigInt.valueSerialCode:
                            record.getColumnValueList().add(new DT_BigInt(randomAccessFile.readLong()));
                            break;

                        case DT_Real.nullSerialCode:
                            record.getColumnValueList().add(new DT_Real(randomAccessFile.readFloat(), true));
                            break;

                        case DT_Real.valueSerialCode:
                            record.getColumnValueList().add(new DT_Real(randomAccessFile.readFloat()));
                            break;

                        case DT_Double.nullSerialCode:
                            record.getColumnValueList().add(new DT_Double(randomAccessFile.readDouble(), true));
                            break;

                        case DT_Double.valueSerialCode:
                            record.getColumnValueList().add(new DT_Double(randomAccessFile.readDouble()));
                            break;

                        //case DT_Date.nullSerialCode is overridden with DT_Double

                        case DT_Date.valueSerialCode:
                            record.getColumnValueList().add(new DT_Date(randomAccessFile.readLong()));
                            break;

                        //case DT_DateTime.nullSerialCode is overridden with DT_Double

                        case DT_DateTime.valueSerialCode:
                            record.getColumnValueList().add(new DT_DateTime(randomAccessFile.readLong()));
                            break;

                        case DT_Text.nullSerialCode:
                            record.getColumnValueList().add(new DT_Text(null));

                        case DT_Text.valueSerialCode:
                            record.getColumnValueList().add(new DT_Text(""));
                            break;

                        default:
                            if (serialTypeCodes[i] > DT_Text.valueSerialCode) {
                                byte length = (byte) (serialTypeCodes[i] - DT_Text.valueSerialCode);
                                char[] text = new char[length];
                                for (byte j = 0; j < length; j++) {
                                    text[j] = (char) randomAccessFile.readByte();
                                }
                                record.getColumnValueList().add(new DT_Text(new String(text)));
                            }
                            break;

                    }
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
