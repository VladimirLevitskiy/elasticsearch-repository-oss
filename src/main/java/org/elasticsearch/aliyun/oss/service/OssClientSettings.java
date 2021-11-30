package org.elasticsearch.aliyun.oss.service;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.simpleString;

/**
 * OSS client configuration
 * Created by yangkongshi on 2017/11/27.
 */
public class OssClientSettings {
    private static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    private static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.GB);

    private static final String EMPTY_STRING = "";

    public static final Setting<SecureString> ACCESS_KEY_ID =
            new Setting<>("access_key_id", EMPTY_STRING, (someString) -> new SecureString(someString.toCharArray()), Property.Filtered,
                    Property.Dynamic, Property.NodeScope);
    public static final Setting<SecureString> SECRET_ACCESS_KEY =
            new Setting<>("secret_access_key", EMPTY_STRING, (someString) -> new SecureString(someString.toCharArray()), Property.Filtered,
                    Property.Dynamic, Property.NodeScope);
    public static final Setting<String> ENDPOINT =
            Setting.simpleString("endpoint", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<SecureString> SECURITY_TOKEN =
            new Setting<>("security_token", EMPTY_STRING, (someString) -> new SecureString(someString.toCharArray()), Property.Filtered,
                    Property.Dynamic, Property.NodeScope);
    public static final Setting<String> BUCKET =
            simpleString("bucket", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> BASE_PATH = simpleString("base_path", Setting.Property.NodeScope,
            Setting.Property.Dynamic);
    public static final Setting<Boolean> COMPRESS =
            boolSetting("compress", false, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<ByteSizeValue> CHUNK_SIZE =
            byteSizeSetting("chunk_size", MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE,
                    Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<ByteSizeValue> BUFFER_SIZE =
            byteSizeSetting("buffer_size", MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE,
                    Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<SecureString> ECS_RAM_ROLE =
            new Setting<>("ecs_ram_role", EMPTY_STRING, (someString) -> new SecureString(someString.toCharArray()), Property.Filtered,
                    Property.Dynamic, Property.NodeScope);
    public static final Setting<SecureString> AUTO_SNAPSHOT_BUCKET =
            new Setting<>("auto_snapshot_bucket", EMPTY_STRING, (someString) -> new SecureString(someString.toCharArray()), Property.Filtered,
                    Property.Dynamic, Property.NodeScope);
    public static final Setting<Boolean> SUPPORT_CNAME =
            boolSetting("support_cname", true, Setting.Property.NodeScope, Setting.Property.Dynamic);
}
