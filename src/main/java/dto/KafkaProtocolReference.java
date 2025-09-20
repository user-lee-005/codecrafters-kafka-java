package dto;

/**
 * Explains the structure and encoding of a flexible Kafka ApiVersions response (version 3+),
 * detailing the core components: Unsigned Varints (U_VARINT), Compact Arrays, and Tagged Fields.
 *
 * <p>Starting with version 3, the ApiVersions response (and many other Kafka messages) adopted a
 * "flexible" message format. This format is designed to be more space-efficient and allow for
 * easier evolution of the protocol without breaking older clients. It achieves this through
 * three key encoding mechanisms.
 *
 * <h2>1. Unsigned Varint (U_VARINT)</h2>
 *
 * <p>A U_VARINT is a variable-length encoding for non-negative integers. Its primary goal is to use
 * fewer bytes for smaller numbers, which are statistically more common. This is a significant
 * optimization over using a fixed 4-byte (32-bit) integer for every number.
 *
 * <h3>Encoding Scheme:</h3>
 * <ul>
 * <li>Each byte is split into two parts: a <b>continuation bit</b> (the most significant bit, MSB)
 * and 7 bits of <b>data</b>.</li>
 * <li>If the <b>continuation bit is 1</b>, it signals that the next byte is also part of the
 * same integer.</li>
 * <li>If the <b>continuation bit is 0</b>, it signals that this is the final byte for the integer.</li>
 * </ul>
 *
 * <h3>Example:</h3>
 * <ul>
 * <li>The number <b>3</b> is encoded as a single byte: {@code 00000011} (Hex: {@code 0x03}). The MSB is 0.</li>
 * <li>The number <b>128</b> requires two bytes. It is encoded as {@code 10000000 00000001} (Hex: {@code 0x80 0x01}).</li>
 * </ul>
 *
 * <h2>2. Compact Arrays</h2>
 *
 * <p>A Compact Array leverages U_VARINTs to encode its length, making it more efficient than
 * older "rigid" arrays that used a fixed 4-byte integer for the length.
 *
 * <h3>Encoding Scheme:</h3>
 * <ul>
 * <li>The length of the array is stored as a <b>U_VARINT</b>.</li>
 * <li>Crucially, the value encoded is {@code array.length + 1}. This is done so that a length of
 * -1 (representing a null array) can be encoded as the single byte {@code 0x00}. A non-null,
 * empty array (length 0) is encoded as {@code 0x01}.</li>
 * </ul>
 *
 * <h3>Example:</h3>
 * <ul>
 * <li>An array with <b>3 elements</b> will have its length encoded as the U_VARINT for the number <b>4</b>
 * (which is the single byte {@code 0x04}).</li>
 * <li>An array with <b>128 elements</b> will have its length encoded as the U_VARINT for <b>129</b>
 * (which is two bytes: {@code 0x81 0x01}).</li>
 * </ul>
 *
 * <h2>3. Tagged Fields (TAG_BUFFER)</h2>
 *
 * <p>Tagged Fields are the mechanism for adding new, optional fields to a message without
 * breaking backward or forward compatibility. Older clients can simply ignore tagged fields
 * they don't understand.
 *
 * <h3>Encoding Scheme:</h3>
 * <ul>
 * <li>A message's main body is followed by a "tag buffer" section.</li>
 * <li>This section starts with a <b>U_VARINT</b> indicating the <b>number of tagged fields</b> that follow.</li>
 * <li>Each tagged field consists of:
 * <ol>
 * <li><b>Tag Number (U_VARINT):</b> A unique ID for the field (e.g., 0, 1, 2...).</li>
 * <li><b>Field Size (U_VARINT):</b> The number of bytes the field's data occupies.</li>
 * <li><b>Field Data (bytes):</b> The actual content of the field.</li>
 * </ol>
 * </li>
 * </ul>
 *
 * <hr>
 *
 * <h2>Structure of ApiVersions Response v3+</h2>
 *
 * <p>Putting it all together, here is the byte-by-byte structure of a modern ApiVersions response.
 *
 * <pre>
 * ApiVersionsResponseV3+ {
 * // --- Main Body ---
 * ErrorCode      : INT16         // 2 bytes. 0 for success.
 * ThrottleTimeMs : INT32         // 4 bytes. Part of the flexible message format.
 * ApiKeys        : COMPACT_ARRAY // A Compact Array of ApiVersion structs.
 * Length     : U_VARINT      // Number of structs + 1.
 * Elements[] : {
 * ApiKey       : INT16   // 2 bytes. The API key (e.g., 18 for ApiVersions).
 * MinVersion   : INT16   // 2 bytes.
 * MaxVersion   : INT16   // 2 bytes.
 * TaggedFields : TAG_BUFFER // Tagged fields for *this struct*.
 * NumTags  : U_VARINT   // Must be present, even if 0. (Usually 0x00).
 * }
 *
 * // --- Tag Buffer for the Top-Level Message ---
 * TaggedFields   : TAG_BUFFER
 * NumTags    : U_VARINT      // Number of top-level tagged fields that follow.
 * // (Usually 0x00 for this specific message type).
 * }
 * </pre>
 *
 * <h3>Key Takeaways for Implementation:</h3>
 * <ul>
 * <li>Always check the request version to decide whether to serialize in the rigid or flexible format.</li>
 * <li>When using the flexible format, remember to correctly calculate the total message size by summing the
 * variable sizes of U_VARINTs and the fixed sizes of other fields.</li>
 * <li>Do not forget to write the tagged fields section (even if it's just a single {@code 0x00} byte for
 * zero tags) for both the top-level message and for any complex structs inside a compact array.</li>
 * </ul>
 *
 * @see <a href="https://kafka.apache.org/protocol#protocol_messages">Kafka Protocol Documentation</a>
 */
public class KafkaProtocolReference {
    // This class is for documentation purposes only.
}