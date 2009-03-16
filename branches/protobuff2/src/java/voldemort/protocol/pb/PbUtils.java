package voldemort.protocol.pb;

import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.ErrorCodeMapper;
import voldemort.utils.ByteArray;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;

public class PbUtils {

    public static VoldemortProtocol.Error.Builder encodeError(ErrorCodeMapper mapper,
                                                              VoldemortException e) {
        return VoldemortProtocol.Error.newBuilder()
                                      .setErrorCode(mapper.getCode(e))
                                      .setErrorMessage(e.getMessage());
    }

    public static VoldemortProtocol.Versioned.Builder encodeVersioned(Versioned<byte[]> versioned) {
        return VoldemortProtocol.Versioned.newBuilder()
                                          .setValue(ByteString.copyFrom(versioned.getValue()))
                                          .setVersion(PbUtils.encodeClock(versioned.getVersion()));
    }

    public static Versioned<byte[]> decodeVersioned(VoldemortProtocol.Versioned versioned) {
        return new Versioned<byte[]>(versioned.getValue().toByteArray(),
                                     decodeClock(versioned.getVersion()));
    }

    public static List<Versioned<byte[]>> decodeVersions(List<VoldemortProtocol.Versioned> versioned) {
        List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>(versioned.size());
        for(VoldemortProtocol.Versioned v: versioned)
            values.add(decodeVersioned(v));
        return values;
    }

    public static VectorClock decodeClock(VoldemortProtocol.VectorClock encoded) {
        List<ClockEntry> entries = new ArrayList<ClockEntry>(encoded.getEntriesCount());
        for(VoldemortProtocol.ClockEntry entry: encoded.getEntriesList())
            entries.add(new ClockEntry((short) entry.getNodeId(), entry.getVersion()));
        return new VectorClock(entries, encoded.getTimestamp());
    }

    public static VoldemortProtocol.VectorClock.Builder encodeClock(Version version) {
        VectorClock clock = (VectorClock) version;
        VoldemortProtocol.VectorClock.Builder encoded = VoldemortProtocol.VectorClock.newBuilder();
        encoded.setTimestamp(clock.getTimestamp());
        for(ClockEntry entry: clock.getEntries())
            encoded.addEntries(VoldemortProtocol.ClockEntry.newBuilder()
                                                           .setNodeId(entry.getNodeId())
                                                           .setVersion(entry.getVersion()));
        return encoded;
    }

    public static ByteArray decodeBytes(ByteString string) {
        return new ByteArray(string.toByteArray());
    }

    public static ByteString encodeBytes(ByteArray array) {
        return ByteString.copyFrom(array.get());
    }

}
