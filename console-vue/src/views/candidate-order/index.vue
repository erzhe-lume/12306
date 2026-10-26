<template>
  <div class="candidate-order-wrapper">
    <Space direction="vertical" :style="{ width: '100%' }" size="large">
      <Card>
        <template #title>
          <div class="title-wrapper">
            <h1>候补订单</h1>
            <h2>（有余票时系统将自动为您兑现）</h2>
          </div>
        </template>
        <div class="tip">
          *提交候补订单后，系统将在有余票时自动为您兑现；兑现成功后席位将被锁定并进入待支付状态，请在规定时间内完成支付，超时未支付席位将自动释放。
        </div>
      </Card>
      <Card>
        <div :style="{ marginBottom: '10px', textAlign: 'end' }">
          <Button :loading="state.loading" @click="loadList">刷新</Button>
        </div>
        <Table
          :columns="columns"
          :data-source="state.dataSource"
          :loading="state.loading"
          :pagination="false"
          :locale="{ emptyText: '暂无候补订单' }"
        >
          <template #status="{ text }">
            <Tag :color="findStatus(text)?.color">
              {{ findStatus(text)?.label ?? '未知' }}
            </Tag>
          </template>
          <template #route="{ record }">
            {{ record.departure }}站 —— {{ record.arrival }}站
          </template>
          <template #createTime="{ text }">
            {{ text ? dayjs(text).format('YYYY-MM-DD HH:mm:ss') : '--' }}
          </template>
          <template #operation="{ record }">
            <Space>
              <!-- 等待中的候补订单支持用户主动取消 -->
              <Popconfirm
                v-if="record.status === 0"
                title="确认取消该候补订单吗？"
                ok-text="确认"
                cancel-text="再想想"
                @confirm="() => handleCancelCandidate(record)"
              >
                <Button type="link" size="small" danger>取消候补</Button>
              </Popconfirm>
              <!-- 兑现成功的候补订单跳转正式订单页完成支付 -->
              <Button
                v-if="record.status === 2 && record.formalOrderSn"
                type="link"
                size="small"
                @click="() => router.push(`/order?sn=${record.formalOrderSn}`)"
                >去支付</Button
              >
              <!-- 已完成的候补订单可跳转查看正式订单 -->
              <Button
                v-if="record.status === 3 && record.formalOrderSn"
                type="link"
                size="small"
                @click="() => router.push(`/order?sn=${record.formalOrderSn}`)"
                >查看订单</Button
              >
            </Space>
          </template>
        </Table>
      </Card>
    </Space>
  </div>
</template>

<script setup>
import {
  Card,
  Space,
  Table,
  Button,
  Tag,
  Popconfirm,
  message
} from 'ant-design-vue'
import { useRouter } from 'vue-router'
import { onMounted, reactive } from 'vue'
import dayjs from 'dayjs'
import { fetchCandidateOrderList, fetchCandidateCancel } from '@/service'
import { CANDIDATE_ORDER_STATUS_LIST } from '@/constants'

const router = useRouter()
const state = reactive({
  loading: false,
  dataSource: []
})

const findStatus = (value) =>
  CANDIDATE_ORDER_STATUS_LIST.find((item) => item.value === value)

const columns = [
  {
    title: '候补订单号',
    dataIndex: 'candidateOrderSn',
    width: 200
  },
  {
    title: '车次',
    dataIndex: 'trainNumber',
    width: 100
  },
  {
    title: '出发站——到达站',
    dataIndex: 'departure',
    slots: { customRender: 'route' }
  },
  {
    title: '状态',
    dataIndex: 'status',
    width: 100,
    slots: { customRender: 'status' }
  },
  {
    title: '备注',
    dataIndex: 'failReason',
    width: 150,
    customRender: ({ text }) => text ?? '--'
  },
  {
    title: '提交时间',
    dataIndex: 'createTime',
    width: 180,
    slots: { customRender: 'createTime' }
  },
  {
    title: '操作',
    dataIndex: '',
    width: 140,
    slots: { customRender: 'operation' }
  }
]

const loadList = () => {
  state.loading = true
  fetchCandidateOrderList()
    .then((res) => {
      if (res.success) {
        state.dataSource = (res.data ?? []).map((item, index) => ({
          ...item,
          key: item.candidateOrderSn ?? index
        }))
      } else {
        message.error(res.message)
      }
      state.loading = false
    })
    .catch((error) => {
      state.loading = false
      console.log(error)
    })
}

const handleCancelCandidate = (record) => {
  fetchCandidateCancel({
    candidateOrderSn: record.candidateOrderSn
  }).then((res) => {
    if (res.success) {
      message.success('候补订单已取消')
      loadList()
    } else {
      message.error(res.message)
    }
  })
}

onMounted(() => {
  loadList()
})
</script>

<style lang="scss" scoped>
.candidate-order-wrapper {
  .title-wrapper {
    display: flex;
    height: 20px;
    line-height: 20px;
    h1 {
      font-size: 16px;
      margin: 0;
      color: #fff;
    }
    h2 {
      font-size: 14px;
      margin: 0;
      color: #fff;
    }
  }
  .tip {
    color: #1e71bd;
  }
}
::v-deep {
  .ant-card-head {
    background-color: #1e71bd;
  }
  .ant-card-body {
    background-color: #ebedf6;
    padding: 10px 15px;
  }
  .ant-table-thead {
    .ant-table-cell {
      background: #f3f3f3 !important;
      background-color: #f3f3f3 !important;
    }
    > tr > th {
      padding: 8px;
    }
  }
}
</style>
