package com.label.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @BelongsProject: lyh_git
 * @BelongsPackage: com.label.domain
 * @Author: liyuhuan
 * @CreateTime: 2025-08-18  18:50
 * @Description: TODO
 * @Version: 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
