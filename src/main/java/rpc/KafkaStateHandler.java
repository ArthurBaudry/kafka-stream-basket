package rpc;

import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.thrift.TException;
import prices.KakfaStateService;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by octo-art on 18/05/2017.
 */
public class KafkaStateHandler implements KakfaStateService.Iface {

    private final KafkaStreams streams;

    public KafkaStateHandler(final KafkaStreams streams) {
        this.streams = streams;
    }

    @Override
    public Map<String, Long> getAll(String store) throws TException {
        return rangeForKeyValueStore(store, ReadOnlyKeyValueStore::all);
    }

    private Map<String, Long> rangeForKeyValueStore(final String store, final Function<ReadOnlyKeyValueStore<String, Long>,
            KeyValueIterator<String, Long>> rangeFunction) {

        // Get the KeyValue Store
        final ReadOnlyKeyValueStore<String, Long> stateStore = streams.store(store, QueryableStoreTypes.keyValueStore());

        if (stateStore == null) {
            throw new NotFoundException(store + " not found !");
        }

        final Map<String, Long> results = new HashMap<>();
        // Apply the function, i.e., query the store
        final KeyValueIterator<String, Long> range = rangeFunction.apply(stateStore);

        // Convert the results
        while (range.hasNext()) {
            final KeyValue<String, Long> next = range.next();
            results.put(next.key, next.value);
        }

        return results;
    }
}
