package com.label.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/15 9:57
 * @version: 1.8
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
