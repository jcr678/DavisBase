package Model;

import QueryParser.DatabaseHelper;

public class Literal {
    public DataType type;
    public String value;

    public static Literal CreateLiteral(String literalString){
        if(literalString.startsWith("'") && literalString.endsWith("'")){
            literalString = literalString.substring(1, literalString.length()-1);
            return new Literal(DataType.TEXT, literalString);
        }

        if(literalString.startsWith("\"") && literalString.endsWith("\"")){
            literalString = literalString.substring(1, literalString.length()-1);
            return new Literal(DataType.TEXT, literalString);
        }

        try{
            Integer.parseInt(literalString);
            return new Literal(DataType.INT, literalString);
        }
        catch (Exception e){}

        try{
            Double.parseDouble(literalString);
            return new Literal(DataType.REAL, literalString);
        }
        catch (Exception e){}

        DatabaseHelper.UnrecognisedCommand(literalString, "Unrecognised Literal Found. Please use integers, real or strings ");
        return null;
    }

    private Literal(DataType type, String value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        if(this.type == DataType.TEXT){
            return this.value;
        }
        else if(this.type == DataType.INT) {
            return this.value;
        }
        else if(this.type == DataType.REAL) {
            return String.format("%.2f", Double.parseDouble(this.value));
        }
        else if(this.type == DataType.INT_REAL_NULL || this.type == DataType.SMALL_INT_NULL || this.type == DataType.TINY_INT_NULL || this.type == DataType.DOUBLE_DATETIME_NULL) {
            return "NULL";
        }

        return "";
    }
}
