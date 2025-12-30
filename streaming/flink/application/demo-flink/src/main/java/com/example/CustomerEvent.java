package com.example;

import java.sql.Timestamp;

public class CustomerEvent {
    public String op;   // c | u | d
    public Integer id;
    public String name;
    public String email;
    public Timestamp created_at;

    public CustomerEvent() {}
}
