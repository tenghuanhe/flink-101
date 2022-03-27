/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.tenghuanhe;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Calendar;

/**
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 *
 * <p>Toggle "Add dependencies with 'provided' scope to classpath" if you'd like to run or debug
 * this application in IDEA or other Java IDE.
 */
public class StreamingJob {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        int cardinality = params.getInt("cardinality", 4);
        int emitInterval = params.getInt("emitInterval", 1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Character> characterDataStream = env.addSource(new RandomCharSource(cardinality, emitInterval));
        DataStream<CharacterCount> windowCounts = characterDataStream.map(ch -> new CharacterCount(ch, 1)).keyBy(row -> row.character).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).reduce((a, b) -> new CharacterCount(a.character, a.count + b.count)).returns(CharacterCount.class);
        System.out.println(env.getExecutionPlan());
        windowCounts.print();
        env.execute("Character count");
    }

    public static class RandomCharSource extends RichParallelSourceFunction<Character> {

        private volatile boolean cancelled = false;

        private final int cardinality;

        private final int emitIntervalMs;

        public RandomCharSource(int cardinality, int emitIntervalMs) {
            this.cardinality = cardinality;
            this.emitIntervalMs = emitIntervalMs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void run(SourceContext<Character> ctx) throws Exception {
            while (!cancelled) {
                ctx.collect((char) (Math.random() * cardinality + 'a'));
                Thread.sleep(emitIntervalMs);
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

    public static class CharacterCount {
        public char character;
        public long count;

        @SuppressWarnings("unused")
        public CharacterCount() {
        }

        public CharacterCount(char character, long count) {
            this.character = character;
            this.count = count;
        }

        @Override
        public String toString() {
            Calendar cal = Calendar.getInstance();
            int hour = cal.get(Calendar.HOUR_OF_DAY), minute = cal.get(Calendar.MINUTE), second = cal.get(Calendar.SECOND);
            return String.format("%02d:%02d:%02d", hour, minute, second) + " " + character + " " + count;
        }
    }
}
