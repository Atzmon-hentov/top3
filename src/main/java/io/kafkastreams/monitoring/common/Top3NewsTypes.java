/*
 * Copyright (c) 2019. Prashant Kumar Pandey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package io.kafkastreams.monitoring.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafkastreams.monitoring.types.ClicksByNewsType;

import java.io.IOException;
import java.util.*;

/**
 * Maintains only top 3 Clicks By New Type
 *
 * @author prashant
 * @author www.learningjournal.guru
 */

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "top3Sorted"
})

public class Top3NewsTypes implements Iterable<ClicksByNewsType> {

    private ObjectMapper mapper = new ObjectMapper();
    private final Map<String, ClicksByNewsType> top3Searchable = new HashMap<>();

    @JsonIgnore
    public final TreeSet<ClicksByNewsType> t3sorted = new TreeSet<>((o1, o2) -> {
        final int result = o2.getClicks().compareTo(o1.getClicks());
        if (result != 0) {
            return result;
        } else {
            return o1.getNewsType().compareTo(o2.getNewsType());
        }

    });

    public void add(final ClicksByNewsType newValue) {
        if (top3Searchable.containsKey(newValue.getNewsType())) {
            t3sorted.remove(top3Searchable.remove(newValue.getNewsType()));
        }
        top3Searchable.put(newValue.getNewsType(), newValue);
        t3sorted.add(newValue);
        if (t3sorted.size() > 3) {
            final ClicksByNewsType lastItem = t3sorted.last();
            top3Searchable.remove(lastItem.getNewsType());
            t3sorted.remove(lastItem);
        }
    }

    public void remove(final ClicksByNewsType oldValue) {
        top3Searchable.remove(oldValue.getNewsType());
        t3sorted.remove(oldValue);
    }

    @Override
    public Iterator<ClicksByNewsType> iterator() {
        return t3sorted.iterator();
    }

    //Add getter and setter for Json Serialization
    @JsonProperty("top3Sorted")
    public String getTop3Sorted() throws JsonProcessingException {
        return mapper.writeValueAsString(t3sorted);
    }

    @JsonProperty("top3Sorted")
    public void setTop3Sorted(String top3Sorted) throws IOException {
        ClicksByNewsType[] top3 = mapper.readValue(top3Sorted, ClicksByNewsType[].class);
        for (ClicksByNewsType i : top3) {
            add(i);
        }
    }
}
