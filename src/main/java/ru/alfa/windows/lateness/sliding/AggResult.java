package ru.alfa.windows.lateness.sliding;

import lombok.*;

@ToString
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AggResult {
    private long sum;
    private long max;
    private double avg;
    private long count;
}
