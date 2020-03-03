import pytest

from reader import parse_line


@pytest.mark.parametrize(
    'input_line, expected_pri, expected_header, expected_msg',
    [(
        '<47>Sep 22 15:38:21 mymachine myproc% fatal error, terminating!',
        47, 'Sep 22 15:38:21 mymachine', 'myproc% fatal error, terminating!',
    ), (
        '<34>Jan 25 05:06:34 10.1.2.3 su: \'su root\' failed for sprinkles on /dev/pts/8',
        34, 'Jan 25 05:06:34 10.1.2.3', 'su: \'su root\' failed for sprinkles on /dev/pts/8',
    ), (
        '<13>Oct  7 10:09:00 unicorn sched# invalid operation',
        13, 'Oct  7 10:09:00 unicorn', 'sched# invalid operation',
    ), (
        '<165>Aug  3 22:14:15 FEDC:BA98:7654:3210:FEDC:BA98:7654:3210 awesomeapp starting up version 3.0.1...',
        165, 'Aug  3 22:14:15 FEDC:BA98:7654:3210:FEDC:BA98:7654:3210', 'awesomeapp starting up version 3.0.1...',
    )]
)
def test_parse_line(input_line, expected_pri, expected_header, expected_msg):
    data = parse_line(input_line)
    assert data['pri'] == expected_pri
    assert data['timestamp'] + ' ' + data['hostname'] == expected_header
    assert data['msg'] == expected_msg
