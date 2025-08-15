package com.label.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/15 9:56
 * @version: 1.8
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {
    private String id;
    private String b3name;
    private String b2name;
    private String b1name;
}
