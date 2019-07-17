package org.dean.flink.domain;

import lombok.Data;
import lombok.ToString;

/**
 * @description: 社群
 * @author: dean
 * @create: 2019/07/17 11:30
 */
@Data
@ToString
public class Community {
    private String groupId;
    private String groupName;
    private String creatorAccountId;
    private String createTime;
    private String imageUrl;
    private String deleteTime;
}
