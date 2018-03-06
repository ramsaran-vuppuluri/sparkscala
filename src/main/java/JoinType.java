public enum JoinType {
    INNER("inner"),
    OUTER("outer"),
    LEFT_OUTER("left_outer"),
    RIGHT_OUTER("right_outer"),
    LEFT_SEMI("left_semi"),
    LEFT_ANTI("left_anti"),
    CROSS("cross");

    private String joingType;

    public String joingType() {
        return joingType;
    }

    JoinType(String joingType) {
        this.joingType = joingType;
    }
}