from pathlib import Path

from fastapi import APIRouter, Query, status

from app.data_generation.defect_transfer_dataset_builder import (
    DEFAULT_DATASET_ROOT,
    generate_defect_transfer_datasets,
)
from app.dto.response import CommonResponse
from app.utils.response_utils import success_response


router = APIRouter(prefix="/api/ml/datasets", tags=["ML Dataset"])


@router.post(
    "/defect-transfer/generate",
    summary="불량 탐지/전이 예측 학습 CSV 생성",
    operation_id="generateDefectTransferTrainingDatasets",
    status_code=status.HTTP_201_CREATED,
)
def generate_defect_transfer_training_datasets(
    car_count: int = Query(
        default=12_000,
        ge=100,
        le=100_000,
        description="생성할 차량 수입니다. 차량 1대당 PRESS/BODY/PAINT/ASSEMBLY 4개 이벤트가 생성됩니다.",
    ),
    train_ratio: float = Query(
        default=0.8,
        gt=0.5,
        lt=0.95,
        description="차량 단위 train split 비율입니다.",
    ),
    output_dir: str | None = Query(
        default=None,
        description="CSV 출력 디렉터리입니다. 생략하면 app/ml/datasets/process/generated 를 사용합니다.",
    ),
) -> CommonResponse[dict]:
    paths = generate_defect_transfer_datasets(
        dataset_root=DEFAULT_DATASET_ROOT,
        output_dir=Path(output_dir) if output_dir else None,
        car_count=car_count,
        train_ratio=train_ratio,
    )
    return success_response(
        data={
            "outputDir": str(paths.output_dir),
            "defectDetectionTrain": str(paths.defect_train),
            "defectDetectionTest": str(paths.defect_test),
            "transferPredictionTrain": str(paths.transfer_train),
            "transferPredictionTest": str(paths.transfer_test),
            "metadata": str(paths.metadata),
            "eventRows": paths.event_rows,
            "transitionRows": paths.transition_rows,
            "trainCars": paths.train_cars,
            "testCars": paths.test_cars,
        },
        message="불량 탐지/전이 예측 학습 CSV 생성이 완료되었습니다.",
    )
