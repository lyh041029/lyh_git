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
public class DimSkuInfoMsg implements Serializable {
    private String skuid;
    private String spuid;
    private String c3id;
    private String tname;
}
