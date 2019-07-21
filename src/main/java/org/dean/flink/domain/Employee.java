package org.dean.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @description: 员工信息
 * @author: dean
 * @create: 2019/07/17 11:11
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Employee {
    private Integer id;
    private String name;
    private String deg;
    private String salary;
    private String dept;
}
