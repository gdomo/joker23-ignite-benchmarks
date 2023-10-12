package com.jokerconf.gdomo.ignite;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;

public class StockOrder {
    private byte[] payload;
    private String secCode = "SBER";
    private Double price = 162.93;
    private Integer quantity = 2000;
    private Character buySell = 'B';
    private String currency = "RUB";
    private String secBoard = "TQBR";
    private String account = "S01-12345678";
    private String user = "MU1234567890";
    private Double commission = 0.13;
    private Character status = 'O';
    private Boolean marketMaker = false;

    private static final int defaultLength = Optional.of(new StockOrder())
            .map(o ->
                    Arrays.stream(StockOrder.class.getDeclaredFields())
                            .peek(f -> f.setAccessible(true))
                            .map(f -> {
                                try {
                                    return f.get(o);
                                } catch (IllegalAccessException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .filter(Objects::nonNull)
                            .mapToInt(v -> {
                                if (v instanceof String s) {
                                    return s.getBytes(StandardCharsets.UTF_8).length;
                                } else if (v instanceof Character) {
                                    return 2;
                                } else if (v instanceof Double) {
                                    return 8;
                                } else if (v instanceof Integer) {
                                    return 4;
                                } else if (v instanceof LocalDate) {
                                    return 4 + 2 + 2;
                                } else if (v instanceof Boolean) {
                                    return 1;
                                } else {
                                    throw new RuntimeException("Unexpected field");
                                }
                            })
                            .sum()
            )
            .orElseThrow();

    public static StockOrder ofBytes(int bytes) {
        final StockOrder pojo = new StockOrder();
        pojo.payload = new byte[bytes - defaultLength];
        return pojo;
    }

    public BinaryObject toBinaryByFields(BinaryObjectBuilder builder) {
        builder.setField("payload", payload, byte[].class);
        builder.setField("secBoard", secBoard, String.class);
        builder.setField("secCode", secCode, String.class);
        builder.setField("buySell", buySell, Character.class);
        builder.setField("currency", currency, String.class);
        builder.setField("price", price, Double.class);
        builder.setField("quantity", quantity, Integer.class);
        builder.setField("account", account, String.class);
        builder.setField("user", user, String.class);
        builder.setField("commission", commission, Double.class);
        builder.setField("status", status, Character.class);
        builder.setField("marketMaker", marketMaker, Boolean.class);

        return builder.build();
    }

    public static LinkedHashMap<String, String> fields() {
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("payload", byte[].class.getName());
        fields.put("secBoard", String.class.getName());
        fields.put("secCode", String.class.getName());
        fields.put("buySell", Character.class.getName());
        fields.put("currency", String.class.getName());
        fields.put("price", Double.class.getName());
        fields.put("quantity", Integer.class.getName());
        fields.put("account", String.class.getName());
        fields.put("user", String.class.getName());
        fields.put("commission", Double.class.getName());
        fields.put("status", Character.class.getName());
        fields.put("marketMaker", Boolean.class.getName());

        return fields;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Integer getQuantity() {
        return quantity;
    }
}
