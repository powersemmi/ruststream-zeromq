import {describe, expect, it} from 'vitest';
import {table} from './table';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const drawn = table(words, {
  lanes: ['handlerLane', 'brokerLane'],
  widths: [112, 236],
  rows: [
    [{id: 'ack', code: true}, {id: 'ackEffect', kind: 'good'}],
    [{id: 'drop', code: true}, {id: 'dropEffect', kind: 'bad'}],
  ],
});

describe('a table drawn as a timeline of columns', () => {
  it('places each row under the one before, its cells side by side across the drawing', () => {
    const at = (id: string) => drawn.boxes.find((box) => box.id === id)!;
    expect(at('ack').y).toBe(at('ackEffect').y);
    expect(at('drop').y).toBeGreaterThan(at('ack').y + at('ack').h);
    expect(at('ack').x).toBe(0);
    expect(at('ackEffect').x + at('ackEffect').w).toBe(360);
    expect(at('ackEffect').x).toBeGreaterThan(at('ack').x + at('ack').w);
  });

  it('links each cell to the next one in its row, and never across rows', () => {
    expect(drawn.links).toEqual([
      {from: 'ack', to: 'ackEffect'},
      {from: 'drop', to: 'dropEffect'},
    ]);
  });

  it('heads each column with its lane, centred over the column', () => {
    expect(drawn.lanes).toEqual([
      {x: 56, label: 'handlerLane'},
      {x: 242, label: 'brokerLane'},
    ]);
  });

  it('keeps the kind a cell names, and the code font where it asks for it', () => {
    const at = (id: string) => drawn.boxes.find((box) => box.id === id)!;
    expect(at('ack')).toMatchObject({kind: 'service', code: true});
    expect(at('dropEffect')).toMatchObject({kind: 'bad', lines: ['dropEffect']});
  });

  it('gives a row the height of its tallest cell', () => {
    const tall = table(words, {
      lanes: ['a', 'b'],
      widths: [112, 236],
      rows: [
        [{id: 'x', rows: 4}, {id: 'y'}],
        [{id: 'z'}, {id: 'w'}],
      ],
    });
    const at = (id: string) => tall.boxes.find((box) => box.id === id)!;
    expect(at('y').h).toBe(at('x').h);
    expect(at('z').y).toBeGreaterThan(at('x').y + at('x').h);
  });
});
