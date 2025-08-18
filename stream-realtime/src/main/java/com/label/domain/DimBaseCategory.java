package com.label.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

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
public class DimBaseCategory implements Serializable {
    private String id;
    private String b3name;
    private String b2name;
    private String b1name;
}
